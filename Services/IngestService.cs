using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories;
using Npgsql;
using Dapper;
using System.Reflection;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// CSVファイル取込サービス
    /// ビジネスフロー: CSV受領 → ルール取得 → 読込・変換 → temp保存 → EAV生成
    /// </summary>
    public class IngestService
    {
        private readonly DataImportService _dataService;
        private readonly IBatchRepository _batchRepository;
        private readonly IProductRepository _productRepository;
        private readonly string _connectionString;

        // 処理中データ保持
        private readonly List<BatchRun> _batchRuns = new();
        private readonly List<TempProductParsed> _tempProducts = new();
        private readonly List<ClProductAttr> _productAttrs = new();
        private readonly List<RecordError> _recordErrors = new();

        public IngestService(
            string connectionString,
            IBatchRepository batchRepository,
            IProductRepository productRepository)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _batchRepository = batchRepository ?? throw new ArgumentNullException(nameof(batchRepository));
            _productRepository = productRepository ?? throw new ArgumentNullException(nameof(productRepository));
            _dataService = new DataImportService(connectionString);
        }

        /// <summary>
        /// CSVファイル取込メイン処理
        /// フロー全体: 1.バッチ起票 → 2.ルール取得 → 3-6.CSV処理 → 7-9.EAV生成 → 10.統計更新
        /// </summary>
        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine($"=== CSV取込開始 ===\nファイル: {filePath}\nGP会社: {groupCompanyCd}\n処理モード: {targetEntity}");

            // 会社コード検証
            await ValidateCompanyCodeAsync(groupCompanyCd);

            // フロー1: バッチ起票
            var batchId = await CreateBatchRunAsync(filePath, groupCompanyCd, targetEntity);

            try
            {
                // フロー2: ファイル取込ルールの取得
                var (importSetting, importDetails) = await FetchImportRulesAsync(groupCompanyCd, targetEntity);

                // フロー3: CSV読み込み前のI/O設定
                var (config, headerRowIndex) = ConfigureCsvReaderSettings(importSetting);

                // フロー4-6: CSV 1行ずつ読込 → 必須チェック → temp保存
                var result = await ReadCsvAndSaveToTempAsync(filePath, batchId, groupCompanyCd,
                                                             headerRowIndex, importDetails, config);

                // フロー7-9: 固定→EAV投影、EAV生成、メタ付与
                await GenerateProductAttributesAsync(batchId, groupCompanyCd, importDetails);

                // フロー10: バッチ統計更新
                await UpdateBatchStatisticsAsync(batchId, result);

                Console.WriteLine($"=== 取込完了 ===\n読込: {result.readCount}\n成功: {result.okCount}\n失敗: {result.ngCount}");
                return batchId;
            }
            catch (Exception ex)
            {
                await MarkBatchAsFailedAsync(batchId, ex.Message);
                throw;
            }
        }

        #region フロー1: バッチ起票

        /// <summary>
        /// フロー1: バッチ起票
        /// - batch_id 採番
        /// - batch_run に idem_key で冪等化レコード作成 (RUNNING)
        /// - started_at = now()
        /// </summary>
        private async Task<string> CreateBatchRunAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            // batch_id 採番
            string batchId = $"BATCH_{DateTime.Now:yyyyMMddHHmmss}_{Guid.NewGuid():N}";
            var fileInfo = new FileInfo(filePath);
            string idemKey = $"{filePath}_{fileInfo.LastWriteTime.Ticks}";

            var batchRun = new BatchRun
            {
                BatchId = batchId,
                IdemKey = idemKey,
                GroupCompanyCd = groupCompanyCd,
                DataKind = targetEntity,
                FileKey = filePath,
                BatchStatus = "RUNNING",
                StartedAt = DateTime.UtcNow,
                CountsJson = "{\"INGEST\":{\"read\":0,\"ok\":0,\"ng\":0}}"
            };

            await _batchRepository.CreateBatchRunAsync(batchRun);
            _batchRuns.Add(batchRun);

            Console.WriteLine($"バッチ起票完了: {batchId}");
            return batchId;
        }

        #endregion

        #region フロー2: ファイル取込ルールの取得

        /// <summary>
        /// フロー2: ファイル取込ルールの取得
        /// - 入力: group_company_cd と target_entity
        /// - m_data_import_setting を探索して有効な profile_id を決定
        /// - 同 profile_id で m_data_import_d を全件取得
        /// - ルール不在/重複は致命的エラー → FAILED
        /// </summary>
        private async Task<(MDataImportSetting, List<MDataImportD>)> FetchImportRulesAsync(
            string groupCompanyCd, string targetEntity)
        {
            string usageNm = $"{groupCompanyCd}-{targetEntity}";
            var importSetting = await _dataService.GetImportSettingAsync(groupCompanyCd, usageNm);

            // is_active チェック
            if (importSetting == null || !importSetting.IsActive)
                throw new Exception($"有効なファイル取込設定が見つかりません: {usageNm}");

            // 列マッピング取得
            var importDetails = await _dataService.GetImportDetailsAsync(importSetting.ProfileId);
            Console.WriteLine($"取込ルール取得完了: ProfileId={importSetting.ProfileId}, 列数={importDetails.Count}");

            return (importSetting, importDetails);
        }

        #endregion

        #region フロー3: CSV読み込み前のI/O設定

        /// <summary>
        /// フロー3: CSV読み込み前のI/O設定
        /// - 文字コード、区切り、ヘッダ行スキップを設定
        /// - header_row_index で指定された行をヘッダーとして読み込み、その後のデータ行のみ処理
        /// </summary>
        private (CsvConfiguration, int) ConfigureCsvReaderSettings(MDataImportSetting importSetting)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true, // ヘッダー行を自動的に読み込む
                Delimiter = importSetting.Delimiter ,
                BadDataFound = context => { },
                MissingFieldFound = null,
                Encoding = GetEncodingFromCharacterCode(importSetting.CharacterCd )
            };

            // header_row_index を返して、後で使用する
            return (config, importSetting.HeaderRowIndex);
        }

        #endregion

        #region フロー4-6: CSV読込 → 必須チェック → temp保存

/// <summary>
/// フロー4-6: CSV読込 → 必須チェック → temp保存
/// - header_row_index で指定された行までスキップし、その行をヘッダーとして読み込む
/// - その後のデータ行を処理 (変換はConfigで既に設定済み)
/// - column_seq = 0: 公司コード注入
/// - column_seq > 0: CSV列番号 (そのままCSV配列インデックスとして使用)
/// </summary>
private async Task<(int readCount, int okCount, int ngCount)> ReadCsvAndSaveToTempAsync(
    string filePath, string batchId, string groupCompanyCd,
    int headerRowIndex, List<MDataImportD> importDetails, CsvConfiguration config)
{
    int readCount = 0, okCount = 0, ngCount = 0;
    using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
    using var csv = new CsvReader(reader, config);

    // フロー4: 表頭行までスキップ
    for (int i = 0; i < headerRowIndex - 1; i++)
    {
        if (!await csv.ReadAsync())
            throw new Exception($"ヘッダー行 {headerRowIndex} まで到達できません");
    }

    // ヘッダー行を読み込む
    if (!await csv.ReadAsync())
        throw new Exception("ヘッダー行が読み込めません");

    csv.ReadHeader(); // 明确读取表头行
    var headers = csv.HeaderRecord;
    if (headers == null || headers.Length == 0)
        throw new Exception("ヘッダー行が空です");

    Console.WriteLine($"ヘッダー取得完了: {headers.Length} 列");

    // 列マッピング検証
    ValidateColumnMappings(importDetails, headers);

    // データ行処理開始
    long dataRowNumber = 0;
    int currentPhysicalLine = headerRowIndex;

    while (await csv.ReadAsync())
    {
        currentPhysicalLine++;
        dataRowNumber++;
        readCount++;

        var record = csv.Parser.Record;
        if (record == null || record.Length == 0)
        {
            RecordError(batchId, dataRowNumber, currentPhysicalLine, "空のレコード", record);
            ngCount++;
            continue;
        }

        try
        {
            // CSV行をtempProductにマッピング
            MapCsvRowToTempProduct(batchId, groupCompanyCd, dataRowNumber, currentPhysicalLine,
                                   record, headers, importDetails);
            okCount++;
        }
        catch (Exception ex)
        {
            RecordError(batchId, dataRowNumber, currentPhysicalLine, ex.Message, record);
            ngCount++;
        }
    }

    // データベース保存 (フロー6: temp への保存)
    await SaveToTempTablesAsync();

    return (readCount, okCount, ngCount);
}

        /// <summary>
        /// 列マッピング検証
        /// column_seq = 0: 公司コード注入 (CSV列不要)
        /// column_seq > 0: CSV列番号 (1始まり、配列インデックスは -1 が必要)
        /// 重要: is_required=true の必須列のみ検証し、オプション列は CSV に存在しなくても許可
        /// </summary>
        private void ValidateColumnMappings(List<MDataImportD> importDetails, string[] headers)
        {
            var errors = new List<string>();
            var requiredCount = 0;
            var optionalCount = 0;

            foreach (var detail in importDetails.OrderBy(d => d.ColumnSeq))
            {
                // column_seq = 0 は公司コード注入なのでスキップ
                if (detail.ColumnSeq == 0) continue;

                // column_seq > 0 は CSV列番号 (1始まり)、配列インデックスは -1
                int csvIndex = detail.ColumnSeq - 1;

                // CSV範囲外チェック: 必須列のみエラーとする
                if (csvIndex < 0 || csvIndex >= headers.Length)
                {
                    if (detail.IsRequired)
                    {
                        errors.Add($"必須列{detail.ColumnSeq} ({detail.AttrCd ?? detail.TargetColumn}) がCSV範囲外 (CSV列数: {headers.Length})");
                        requiredCount++;
                    }
                    else
                    {
                        optionalCount++;
                        Console.WriteLine($"  [INFO] オプション列{detail.ColumnSeq} ({detail.AttrCd ?? detail.TargetColumn}) はCSVに存在しません (スキップ)");
                    }
                }
            }

            Console.WriteLine($"列マッピング検証完了: CSV列数={headers.Length}, 必須列エラー={requiredCount}, オプション列スキップ={optionalCount}");

            if (errors.Any())
                throw new Exception($"列マッピングエラー (必須列のみ):\n{string.Join("\n", errors)}");
        }

        /// <summary>
        /// フロー4-5: CSV行をtempProductにマッピング + 必須チェック
        /// - column_seq = 0: 公司コード注入
        /// - column_seq > 0: CSV列番号 (1始まり、配列インデックスは -1 が必要)
        /// - transform_expr 適用 (trim(@))
        /// - is_required チェック
        /// - CSV範囲外の列はスキップ (オプション列のみ)
        /// </summary>
        private void MapCsvRowToTempProduct(
            string batchId, string groupCompanyCd, long dataRowNumber, int currentPhysicalLine,
            string[] record, string[] headers, List<MDataImportD> importDetails)
        {
            var tempProduct = new TempProductParsed
            {
                TempRowId = Guid.NewGuid(),
                BatchId = batchId,
                LineNo = dataRowNumber,
                SourceGroupCompanyCd = groupCompanyCd,
                StepStatus = "READY",
                ExtrasJson = "{}"
            };

            var extrasDict = new Dictionary<string, object>();
            var sourceRawDict = new Dictionary<string, string>();
            var requiredFieldErrors = new List<string>();

            foreach (var detail in importDetails.OrderBy(d => d.ColumnSeq))
            {
                string? rawValue = null;
                string? transformedValue = null;
                string headerName = "N/A";
                bool isInjectedValue = false;

                // column_seq = 0: 公司コード注入
                if (detail.ColumnSeq == 0)
                {
                    rawValue = transformedValue = groupCompanyCd;
                    headerName = "[注入:group_company_cd]";
                    isInjectedValue = true;
                }
                // column_seq > 0: CSV列番号 (1始まり)、配列インデックスは -1
                else if (detail.ColumnSeq > 0)
                {
                    int csvIndex = detail.ColumnSeq - 1;  // CSV列番号をインデックスに変換

                    // CSV範囲外チェック: ヘッダー範囲とレコード範囲の両方をチェック
                    if (csvIndex >= headers.Length || csvIndex >= record.Length)
                    {
                        // CSV範囲外の列はスキップ (オプション列として扱う)
                        // 必須列の場合は後の必須チェックでエラーになる
                        continue;
                    }

                    rawValue = record[csvIndex];
                    headerName = headers[csvIndex];
                    // transform_expr 適用
                    transformedValue = ApplyTransformExpression(rawValue, detail.TransformExpr ?? "");
                }
                else
                {
                    continue;
                }

                // フロー5: 必須チェック (is_required)
                if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                {
                    string fieldName = detail.AttrCd ?? detail.TargetColumn ?? $"列{detail.ColumnSeq}";
                    requiredFieldErrors.Add($"{fieldName} (列{detail.ColumnSeq})");
                }

                // 元CSV値を source_raw として保持
                string backupKey = isInjectedValue ? $"_injected_{detail.TargetColumn}" : headerName;
                sourceRawDict[backupKey] = rawValue ?? "";

                // データ格納 (固定フィールド or EAV準備)
                bool? mappingSuccess = null;

                // 固定フィールドへマッピング
                if (!string.IsNullOrEmpty(detail.TargetColumn) &&
                    (detail.TargetEntity == "PRODUCT_MST" || detail.TargetEntity == "PRODUCT"))
                {
                    string propertyName = "Source" + ConvertToPascalCase(detail.TargetColumn);
                    mappingSuccess = SetTempProductProperty(tempProduct, propertyName, transformedValue);
                }

                // EAV ターゲット生成準備 (Step B-1: CSV 侧指定了 attr_cd 的列)
                if (detail.TargetEntity == "EAV" && !string.IsNullOrEmpty(detail.AttrCd))
                {
                    // 支持多值分割 (例: "Red, Blue, Green" → 3条记录)
                    CreateEavAttribute(batchId, tempProduct.TempRowId, detail, detail.ColumnSeq,
                      headerName, transformedValue, isInjectedValue);
                }

                // extras_json 用の詳細情報保存
                extrasDict[$"col_{detail.ColumnSeq}"] = new
                {
                    csv_column_index = detail.ColumnSeq,
                    header = headerName,
                    raw_value = rawValue ?? "",
                    transformed_value = transformedValue ?? "",
                    target_column = detail.TargetColumn ?? "",
                    target_entity = detail.TargetEntity ?? "",
                    attr_cd = detail.AttrCd ?? "",
                    transform_expr = detail.TransformExpr ?? "",
                    is_required = detail.IsRequired,
                    is_injected = isInjectedValue,
                    mapping_success = mappingSuccess
                };
            }

            // 必須チェック結果
            if (requiredFieldErrors.Any())
                throw new Exception($"必須項目エラー: {string.Join(", ", requiredFieldErrors)}");

            // extras_json 最終化
            tempProduct.ExtrasJson = JsonSerializer.Serialize(new
            {
                source_raw = sourceRawDict,
                processed_columns = extrasDict,
                csv_headers = headers,
                physical_line = currentPhysicalLine,
                data_row_number = dataRowNumber,
                processing_timestamp = DateTime.UtcNow
            }, new JsonSerializerOptions { WriteIndented = false });

            _tempProducts.Add(tempProduct);
        }

        /// <summary>
        /// フロー8: EAV ターゲット生成 (第2種：EAV属性)
        /// - m_data_import_d.target_entity='EAV' の各行は 1セル=1属性
        /// - source_raw は CSV値(トリム後)
        /// - value_* フィールドは INGEST 段階では未設定（CLEANSE 段階で設定）
        /// - data_type は未確定（CLEANSE 段階で m_attr_definition と比較して確定）
        /// </summary>
        private void CreateEavAttribute(
            string batchId, Guid tempRowId, MDataImportD detail,
            int csvColumnIndex, string headerName, string? transformedValue, bool isInjectedValue)
        {
            var attrSeq = (short)(_productAttrs.Count(p =>
                p.TempRowId == tempRowId && p.AttrCd == detail.AttrCd) + 1);

            var productAttr = new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempRowId,
                AttrCd = detail.AttrCd,
                AttrSeq = attrSeq,
                SourceId = $"col_{csvColumnIndex}",
                SourceLabel = headerName,
                SourceRaw = transformedValue ?? "",  // CSV 原始值（经过 trim）
                ValueText = null,  // INGEST 段階では未設定 (CLEANSE で設定)
                ValueNum = null,
                ValueDate = null,
                ValueCd = null,
                GListItemId = null,
                DataType = null,   // INGEST 段階では未確定 (CLEANSE で m_attr_definition と比較して確定)
                QualityFlag = "OK",  // 初始化为 OK
                QualityDetailJson = JsonSerializer.Serialize(new
                {
                    empty_value = string.IsNullOrWhiteSpace(transformedValue),
                    processing_stage = "INGEST",
                    is_required = detail.IsRequired
                }),
                ProvenanceJson = JsonSerializer.Serialize(new
                {
                    stage = "INGEST",
                    from = isInjectedValue ? "INJECTED" : "CSV",
                    via = "eav_map",
                    target_entity = "EAV",
                    profile_id = detail.ProfileId,
                    column_seq = csvColumnIndex,
                    csv_header = headerName,
                    transform_expr = detail.TransformExpr ?? ""
                }),
                RuleVersion = "1.0"
            };

            _productAttrs.Add(productAttr);
        }

        /// <summary>
        /// フロー8 : EAV ターゲット生成 - 支持多值分割
        /// - 支持多值分割（例: "Red, Blue, Green" → attr_seq = 1, 2, 3）
        /// - source_raw 保存完整的原始值
        /// - value_* フィールドは INGEST 段階では未設定
        /// </summary>
        /*private void CreateEavAttributesWithSplit(
            string batchId, Guid tempRowId, MDataImportD detail,
            int csvColumnIndex, string headerName, string? transformedValue, bool isInjectedValue)
        {
            if (string.IsNullOrWhiteSpace(transformedValue))
            {
                // 空值情况：创建一条空记录
                CreateSingleEavAttribute(batchId, tempRowId, detail, csvColumnIndex,
                                       headerName, "", isInjectedValue, transformedValue ?? "");
                return;
            }

            // 检查是否需要多值分割
            var splitDelimiters = new[] { ',', ';', '、' };
            var values = transformedValue.Split(splitDelimiters, StringSplitOptions.RemoveEmptyEntries)
                                        .Select(v => v.Trim())
                                        .Where(v => !string.IsNullOrEmpty(v))
                                        .ToList();

            if (values.Count == 0)
            {
                CreateSingleEavAttribute(batchId, tempRowId, detail, csvColumnIndex,
                                       headerName, "", isInjectedValue, transformedValue);
            }
            else if (values.Count == 1)
            {
                CreateSingleEavAttribute(batchId, tempRowId, detail, csvColumnIndex,
                                       headerName, values[0], isInjectedValue, transformedValue);
            }
            else
            {
                // 多值：生成多条记录
                for (int i = 0; i < values.Count; i++)
                {
                    CreateSingleEavAttribute(batchId, tempRowId, detail, csvColumnIndex,
                                           headerName, values[i], isInjectedValue, transformedValue, (short)(i + 1));
                }
            }
        }*/

        /// <summary>
        /// 创建单条 EAV 属性记录
        /// </summary>
        private void CreateSingleEavAttribute(
            string batchId, Guid tempRowId, MDataImportD detail,
            int csvColumnIndex, string headerName, string splitValue,
            bool isInjectedValue, string sourceRaw, short? attrSeqOverride = null)
        {
            var attrSeq = attrSeqOverride ?? (short)(_productAttrs.Count(p =>
                p.TempRowId == tempRowId && p.AttrCd == detail.AttrCd) + 1);

            var productAttr = new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempRowId,
                AttrCd = detail.AttrCd,
                AttrSeq = attrSeq,
                SourceId = $"col_{csvColumnIndex}",
                SourceLabel = headerName,
                SourceRaw = sourceRaw,  // 保存完整的原始值（未分割前）
                ValueText = null,
                ValueNum = null,
                ValueDate = null,
                ValueCd = null,
                GListItemId = null,
                DataType = null,
                QualityFlag = "OK",
                QualityDetailJson = JsonSerializer.Serialize(new
                {
                    empty_value = string.IsNullOrWhiteSpace(splitValue),
                    split_value = splitValue,  // 分割后的单个值
                    processing_stage = "INGEST",
                    is_required = detail.IsRequired,
                    attr_seq = attrSeq
                }),
                ProvenanceJson = JsonSerializer.Serialize(new
                {
                    stage = "INGEST",
                    from = isInjectedValue ? "INJECTED" : "CSV",
                    via = "eav_map",
                    target_entity = "EAV",
                    profile_id = detail.ProfileId,
                    column_seq = csvColumnIndex,
                    csv_header = headerName,
                    transform_expr = detail.TransformExpr ?? "",
                    split_index = attrSeq - 1
                }),
                RuleVersion = "1.0"
            };

            _productAttrs.Add(productAttr);
        }

        #endregion

        #region フロー7-9: 固定→EAV投影、EAV生成、メタ付与

        /// <summary>
        /// フロー7-9: 属性マッピングと cl_product_attr 作成
        /// 7. m_fixed_to_attr_map の適用 (固定→項目コード投影)
        /// 8. EAV ターゲット生成 (既にフロー4-6で処理済み)
        /// 9. 補助キー・メタの付与 (batch_id, temp_row_id, attr_seq)
        /// </summary>
        private async Task GenerateProductAttributesAsync(
            string batchId, string groupCompanyCd, List<MDataImportD> importDetails)
        {
            // フロー7: m_fixed_to_attr_map の適用
            var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT");

            foreach (var tempProduct in _tempProducts)
            {
                // フロー7: 固定フィールド → attr_cd 投影
                // 重要: m_fixed_to_attr_map を基準に反復（重複生成を防ぐ）
                foreach (var attrMap in attrMaps)
                {
                    ProjectFixedFieldToAttribute(batchId, tempProduct, attrMap);
                }
            }

            // データベース保存
            await _productRepository.SaveProductAttributesAsync(_productAttrs);
            Console.WriteLine($"cl_product_attr保存完了: {_productAttrs.Count} レコード");
        }

        /// <summary>
        /// フロー7: 固定フィールド → 項目コード投影 (第1種：PRODUCT_MST 固定列)
        /// - source_id: m_fixed_to_attr_map.source_id_column で指定された列の値を取得
        /// - source_label: m_fixed_to_attr_map.source_label_column で指定された列の値を取得
        /// - source_raw: ID列とLabel列を JSON 形式で組み合わせて保存
        /// - value_* フィールドは INGEST 段階では未設定（CLEANSE 段階で設定）
        /// </summary>
        private void ProjectFixedFieldToAttribute(
            string batchId, TempProductParsed tempProduct, MFixedToAttrMap attrMap)
        {
            short attrSeqForRow = (short)(_productAttrs.Count(p =>
                p.TempRowId == tempProduct.TempRowId && p.AttrCd == attrMap.AttrCd) + 1);

            // 1. 获取 source_id 的值（从 temp_product_parsed 的指定列）
            string? sourceIdValue = null;
            if (!string.IsNullOrEmpty(attrMap.SourceIdColumn))
            {
                string idFieldName = "Source" + ConvertToPascalCase(attrMap.SourceIdColumn);
                sourceIdValue = GetTempProductProperty(tempProduct, idFieldName);
            }

            // 2. 获取 source_label 的值（从 temp_product_parsed 的指定列）
            string? sourceLabelValue = null;
            if (!string.IsNullOrEmpty(attrMap.SourceLabelColumn))
            {
                string labelFieldName = "Source" + ConvertToPascalCase(attrMap.SourceLabelColumn);
                sourceLabelValue = GetTempProductProperty(tempProduct, labelFieldName);
            }

            // 3. 构建 source_raw 的 JSON（包含 ID 和 Label 的原始字段名和值）
            var sourceRawDict = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(attrMap.SourceIdColumn))
            {
                sourceRawDict[attrMap.SourceIdColumn] = sourceIdValue ?? "";
            }

            if (!string.IsNullOrEmpty(attrMap.SourceLabelColumn))
            {
                sourceRawDict[attrMap.SourceLabelColumn] = sourceLabelValue ?? "";
            }

            string sourceRaw = JsonSerializer.Serialize(sourceRawDict, new JsonSerializerOptions { WriteIndented = false });

            // 4. 检查是否有值（ID 或 Label 至少有一个有值）
            bool hasValue = !string.IsNullOrEmpty(sourceIdValue) || !string.IsNullOrEmpty(sourceLabelValue);

            // 5. 如果有值，创建属性记录
            if (hasValue)
            {
                var productAttr = new ClProductAttr
                {
                    BatchId = batchId,
                    TempRowId = tempProduct.TempRowId,
                    AttrCd = attrMap.AttrCd,  // 使用 attrMap 的 attr_cd
                    AttrSeq = attrSeqForRow,
                    SourceId = sourceIdValue ?? "",       // ID 列的值
                    SourceLabel = sourceLabelValue ?? "", // Label 列的值
                    SourceRaw = sourceRaw,                // JSON 格式: {"source_brand_id":"4952","source_brand_nm":"ROLEX"}
                    ValueText = null,                     // INGEST 段階では未設定
                    ValueNum = null,
                    ValueDate = null,
                    ValueCd = null,
                    GListItemId = null,
                    DataType = attrMap.DataTypeOverride,  // m_fixed_to_attr_map で指定されたタイプ
                    QualityFlag = "OK",
                    QualityDetailJson = JsonSerializer.Serialize(new
                    {
                        empty_value = !hasValue,
                        processing_stage = "INGEST",
                        is_required = false,  // m_fixed_to_attr_map には is_required がない
                        source_id_column = attrMap.SourceIdColumn,
                        source_label_column = attrMap.SourceLabelColumn
                    }),
                    ProvenanceJson = JsonSerializer.Serialize(new
                    {
                        stage = "INGEST",
                        from = "PRODUCT_MST",
                        via = "fixed_map",
                        target_entity = "PRODUCT_MST",
                        map_id = attrMap.MapId,
                        source_id_column = attrMap.SourceIdColumn,
                        source_label_column = attrMap.SourceLabelColumn
                    }),
                    RuleVersion = "1.0"
                };

                _productAttrs.Add(productAttr);
            }
        }

        /// <summary>
        /// extras_json から processed_columns を抽出
        /// </summary>
        private Dictionary<string, object> ExtractProcessedColumns(Dictionary<string, object> extrasRoot)
        {
            if (extrasRoot.ContainsKey("processed_columns") && extrasRoot["processed_columns"] != null)
            {
                try
                {
                    return JsonSerializer.Deserialize<Dictionary<string, object>>(
                        extrasRoot["processed_columns"].ToString() ?? "{}") ?? new Dictionary<string, object>();
                }
                catch { }
            }
            return new Dictionary<string, object>();
        }

        #endregion

        #region フロー10: バッチ統計更新

        /// <summary>
        /// フロー10: バッチ統計更新
        /// - batch_run.counts_json の read/ok/ng 更新
        /// - batch_status を SUCCESS or PARTIAL に更新
        /// - ended_at = now()
        /// </summary>
        private async Task UpdateBatchStatisticsAsync(string batchId, (int readCount, int okCount, int ngCount) result)
        {
            var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
            if (batchRun != null)
            {
                batchRun.CountsJson = JsonSerializer.Serialize(new
                {
                    INGEST = new { read = result.readCount, ok = result.okCount, ng = result.ngCount },
                    CLEANSE = new { },
                    UPSERT = new { },
                    CATALOG = new { }
                });

                batchRun.BatchStatus = result.ngCount > 0 ? "PARTIAL" : "SUCCESS";
                batchRun.EndedAt = DateTime.UtcNow;

                await _batchRepository.UpdateBatchRunAsync(batchRun);
                Console.WriteLine($"バッチ統計更新完了: {batchRun.BatchStatus}");
            }
        }

        /// <summary>
        /// バッチ失敗マーク
        /// </summary>
        private async Task MarkBatchAsFailedAsync(string batchId, string errorMessage)
        {
            var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
            if (batchRun != null)
            {
                batchRun.BatchStatus = "FAILED";
                batchRun.EndedAt = DateTime.UtcNow;
                await _batchRepository.UpdateBatchRunAsync(batchRun);
                Console.WriteLine($"バッチ失敗: {errorMessage}");
            }
        }

        #endregion

        #region データベース保存 (Repository 経由)

        /// <summary>
        /// フロー6: temp への保存 (Repository 経由)
        /// </summary>
        private async Task SaveToTempTablesAsync()
        {
            await _productRepository.SaveTempProductsAsync(_tempProducts);
            await _productRepository.SaveProductAttributesAsync(_productAttrs);
            await _productRepository.SaveRecordErrorsAsync(_recordErrors);

            Console.WriteLine($"temp保存完了: 商品={_tempProducts.Count}, 属性={_productAttrs.Count}, エラー={_recordErrors.Count}");
        }

        #endregion

        #region ヘルパーメソッド

        /// <summary>
        /// エラーレコード記録
        /// </summary>
        private void RecordError(string batchId, long dataRowNumber, int currentPhysicalLine,
                                string errorMessage, string[]? record)
        {
            var error = new RecordError
            {
                BatchId = batchId,
                Step = "INGEST",
                RecordRef = $"data_row:{dataRowNumber}",
                ErrorCd = errorMessage.Contains("必須項目") ? "MISSING_REQUIRED_FIELD" : "PARSE_FAILED",
                ErrorDetail = $"データ行 {dataRowNumber} (物理行 {currentPhysicalLine}): {errorMessage}",
                RawFragment = string.Join(",", record?.Take(5) ?? Array.Empty<string>())
            };

            _recordErrors.Add(error);
        }

        /// <summary>
        /// transform_expr 適用 (基本は trim(@))
        /// </summary>
        private string? ApplyTransformExpression(string? value, string transformExpr)
        {
            if (string.IsNullOrEmpty(value)) return value;

            var result = value.Trim().Trim('\u3000'); // 全角スペースもトリム

            if (!string.IsNullOrEmpty(transformExpr))
            {
                if (transformExpr.Contains("trim(@)")) result = result.Trim();
                if (transformExpr.Contains("upper(@)")) result = result.ToUpper();
                if (transformExpr.Contains("lower(@)")) result = result.ToLower();
            }

            return result;
        }

        /// <summary>
        /// TempProductParsed プロパティ値設定
        /// </summary>
        private bool SetTempProductProperty(TempProductParsed obj, string propertyName, string? value)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(propertyName);
                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    return true;
                }

                property = typeof(TempProductParsed).GetProperty(propertyName,
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    return true;
                }

                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// TempProductParsed プロパティ値取得
        /// </summary>
        private string? GetTempProductProperty(TempProductParsed obj, string propertyName)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(
                    propertyName,
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance
                );

                return property?.GetValue(obj) as string;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// snake_case → PascalCase 変換
        /// </summary>
        private string ConvertToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var parts = input.Split(new char[] { '_', '-' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Concat(parts.Select(part =>
                char.ToUpperInvariant(part[0]) + part.Substring(1).ToLowerInvariant()));
        }

        /// <summary>
        /// GP会社コード検証
        /// </summary>
        private async Task ValidateCompanyCodeAsync(string groupCompanyCd)
        {
            if (string.IsNullOrWhiteSpace(groupCompanyCd))
                throw new ArgumentException("GP会社コードが指定されていません", nameof(groupCompanyCd));

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var sql = @"
                    SELECT group_company_id as GroupCompanyId, group_company_cd as GroupCompanyCd,
                           group_company_nm as GroupCompanyNm, default_currency_cd as DefaultCurrencyCd,
                           is_active as IsActive, cre_at as CreAt, upd_at as UpdAt
                    FROM m_company
                    WHERE group_company_cd = @GroupCompanyCd AND is_active = true";

                var company = await connection.QueryFirstOrDefaultAsync<MCompany>(
                    sql, new { GroupCompanyCd = groupCompanyCd });

                if (company == null)
                    throw new Exception($"GP会社コードが存在しないか無効です: {groupCompanyCd}");

                if (!company.IsValid())
                    throw new Exception($"GP会社コードのデータが無効です: {groupCompanyCd}");

                Console.WriteLine($"GP会社検証成功: {company.GroupCompanyCd} - {company.GroupCompanyNm}");
            }
            catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P01")
            {
                await ValidateCompanyCodeSimpleAsync(groupCompanyCd);
            }
            catch (Exception ex) when (ex is not ImportException)
            {
                await ValidateCompanyCodeSimpleAsync(groupCompanyCd);
            }
        }

        /// <summary>
        /// GP会社コード簡易検証
        /// </summary>
        private async Task ValidateCompanyCodeSimpleAsync(string groupCompanyCd)
        {
            var validCompanyCodes = new[] { "KM", "RKE", "KBO" };

            if (!validCompanyCodes.Contains(groupCompanyCd.ToUpper()))
                throw new Exception($"GP会社コードが認識されません: {groupCompanyCd}");

            Console.WriteLine($"GP会社コード簡易検証: {groupCompanyCd}");
            await Task.CompletedTask;
        }

        /// <summary>
        /// 文字コード取得
        /// </summary>
        private Encoding GetEncodingFromCharacterCode(string characterCd)
        {
            return characterCd?.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                _ => Encoding.UTF8
            };
        }

        #endregion
    }
}
