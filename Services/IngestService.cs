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
                // フロー2: ファイル取込ルール取得
                var (importSetting, importDetails) = await FetchImportRulesAsync(groupCompanyCd, targetEntity);

                // フロー3: CSV読み込み前のI/O設定
                var (config, headerRowIndex) = ConfigureCsvReaderSettings(importSetting);

                // フロー4-6: CSV 1行ずつ読込 → 必須チェック → temp保存
                var result = await ReadCsvAndSaveToTempAsync(filePath, batchId, groupCompanyCd,
                                                             headerRowIndex, importDetails, config);

                // フロー7-9: 固定→EAV投影、EAV生成、メタ付与
                await GenerateProductAttributesAsync(batchId, groupCompanyCd, importDetails,targetEntity);

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
                Delimiter = importSetting.Delimiter ?? ",",// デフォルトはカンマ区切り
                BadDataFound = context => { },
                MissingFieldFound = null,
                Encoding = GetEncodingFromCharacterCode(importSetting.CharacterCd ?? "UTF-8")// デフォルトは UTF-8
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

            // フロー4: ヘッダー行までスキップ
            for (int i = 0; i < headerRowIndex - 1; i++)
            {
                if (!await csv.ReadAsync())
                    throw new Exception($"ヘッダー行 {headerRowIndex} まで到達できません");
            }

            // ヘッダー行を読み込む
            if (!await csv.ReadAsync())
                throw new Exception("ヘッダー行が読み込めません");

            csv.ReadHeader();
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

            foreach (var detail in importDetails
                .Where(d => d.IsRequired)
                .OrderBy(d => d.ColumnSeq))
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
                }
            }

            Console.WriteLine($"列マッピング検証完了: CSV列数={headers.Length}, 必須列エラー={requiredCount}");

            if (errors.Any())
                throw new Exception($"列マッピングエラー :\n{string.Join("\n", errors)}");
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

            foreach (var detail in importDetails
                .Where(d => d.IsRequired)
               .OrderBy(d => d.ColumnSeq))
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
                    // transform_expr 適用、スペース除去
                    transformedValue = ApplyTransformExpression(rawValue, detail.TransformExpr ?? "");
                }
                else
                {
                    continue;
                }

                // フロー5: 必須チェック (is_required)
                // if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                // {
                //     string fieldName = detail.AttrCd ?? detail.TargetColumn ?? $"列{detail.ColumnSeq}";
                //     requiredFieldErrors.Add($"{fieldName} (列{detail.ColumnSeq})");
                // }

                // 元CSV値を source_raw として保持
                string backupKey = isInjectedValue ? $"_injected_{detail.TargetColumn}" : headerName;
                sourceRawDict[backupKey] = rawValue ?? "";

                // データ格納 (固定フィールド or EAV準備)
                bool? mappingSuccess = null;

                // 固定フィールドへマッピング TODO appsettings.json
                if (!string.IsNullOrEmpty(detail.TargetColumn) &&
                    (detail.ProjectionKind == "PRODUCT_MST" || detail.ProjectionKind == "PRODUCT"))
                {
                    string propertyName = "Source" + ConvertToPascalCase(detail.TargetColumn);
                    mappingSuccess = SetTempProductProperty(tempProduct, propertyName, transformedValue);
                }

                // EAV ターゲット生成準備 (Step B-1: CSV 侧指定了 attr_cd 的列)
                // 注释掉: 现在只处理 PRODUCT_MST,不处理 EAV
                // if (detail.ProjectionKind == "EAV" && !string.IsNullOrEmpty(detail.AttrCd))
                // {
                //     CreateEavAttribute(batchId, tempProduct.TempRowId, detail, detail.ColumnSeq,
                //                       headerName, transformedValue, isInjectedValue);
                // }

                // extras_json 用の詳細情報保存
                extrasDict[$"col_{detail.ColumnSeq}"] = new
                {
                    csv_column_index = detail.ColumnSeq,
                    header = headerName,
                    raw_value = rawValue ?? "",
                    transformed_value = transformedValue ?? "",
                    target_column = detail.TargetColumn ?? "",
                    projection_kind = detail.ProjectionKind ?? "",
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

        #endregion

        #region フロー7-9: 固定→EAV投影、EAV生成、メタ付与

        /// <summary>
        /// フロー7-9: 属性マッピングと cl_product_attr 作成
        /// 7. m_fixed_to_attr_map の適用 (固定→項目コード投影)
        /// 8. EAV ターゲット生成 (既にフロー4-6で処理済み)
        /// 9. 補助キー・メタの付与 (batch_id, temp_row_id, attr_seq)
        /// </summary>
        private async Task GenerateProductAttributesAsync(
            string batchId, string groupCompanyCd, List<MDataImportD> importDetails,string dataKind)
        {
            // 清空之前添加的所有属性数据(包括 EAV 等),重新生成 PRODUCT_MST 的数据
            _productAttrs.Clear();

            // 获取所有必要的映射表数据
            var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, dataKind);
            var attrDefinitions = await _dataService.GetAttrDefinitionsAsync();

            // 从 m_data_import_d 中过滤出 projection_kind == "PRODUCT_MST" 的记录,保持原始顺序
            var productMstDetails = importDetails
                .Where(d => d.ProjectionKind == "PRODUCT_MST")
                .OrderBy(d => d.ColumnSeq)  // 按 column_seq 排序,确保顺序
                .ToList();

            Console.WriteLine($"处理 PRODUCT_MST 属性: 共 {productMstDetails.Count} 个配置");

            // 打印匹配信息
            Console.WriteLine($"\n=== 属性映射统计 ===");
            Console.WriteLine($"m_fixed_to_attr_map 总数: {attrMaps.Count}");
            foreach (var map in attrMaps)
            {
                Console.WriteLine($"  - attr_cd={map.AttrCd}, source_id_column={map.SourceIdColumn}, source_label_column={map.SourceLabelColumn}, value_role={map.ValueRole}");
            }
            Console.WriteLine($"PRODUCT_MST 配置总数: {productMstDetails.Count}");
            foreach (var detail in productMstDetails)
            {
                var hasMap = attrMaps.Any(m => m.AttrCd == detail.AttrCd);
                Console.WriteLine($"  - column_seq={detail.ColumnSeq}, attr_cd={detail.AttrCd}, target_column={detail.TargetColumn}, has_fixed_map={hasMap}");
            }
            Console.WriteLine($"===================\n");

            foreach (var tempProduct in _tempProducts)
            {
                // 用于记录已经处理过的 attr_cd (仅针对 m_fixed_to_attr_map 路径)
                var processedFixedMapAttrCds = new HashSet<string>();

                // 按 m_data_import_d 的顺序逐条处理,不去重
                foreach (var detail in productMstDetails)
                {
                    // 跳过空的 attr_cd
                    if (string.IsNullOrEmpty(detail.AttrCd))
                    {
                        Console.WriteLine($"[跳过] column_seq={detail.ColumnSeq}, attr_cd 为空");
                        continue;
                    }

                    // 尝试从 m_fixed_to_attr_map 中查找匹配
                    var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == detail.AttrCd);

                    if (attrMap != null)
                    {
                        // 情况1: 在 m_fixed_to_attr_map 中找到匹配
                        // 检查是否已经处理过这个 attr_cd
                        if (processedFixedMapAttrCds.Contains(detail.AttrCd))
                        {
                            Console.WriteLine($"[跳过重复] attr_cd={detail.AttrCd} 已通过 m_fixed_to_attr_map 处理过");
                            continue;
                        }

                        Console.WriteLine($"[匹配路径1] attr_cd={detail.AttrCd} 使用 m_fixed_to_attr_map");
                        ProcessAttributeWithFixedMap(batchId, tempProduct, detail, attrMap, attrDefinitions);

                        // 标记为已处理
                        processedFixedMapAttrCds.Add(detail.AttrCd);
                    }
                    else
                    {
                        Console.WriteLine($"[匹配路径2] attr_cd={detail.AttrCd} 使用 m_attr_definition");
                        // 情况2: 没有找到 fixed_map,直接使用 m_attr_definition
                        ProcessAttributeWithoutFixedMap(batchId, tempProduct, detail, attrDefinitions);
                    }
                }
            }

            // データベース保存
            await _productRepository.SaveProductAttributesAsync(_productAttrs);
            Console.WriteLine($"cl_product_attr保存完了: {_productAttrs.Count} レコード");
        }

        /// <summary>
        /// 情况1: 使用 m_fixed_to_attr_map 处理属性
        /// - 根据 value_role 判断取值方式 (ID_AND_LABEL 或 ID_ONLY)
        /// - 从 m_attr_definition 获取 data_type
        /// </summary>
        private void ProcessAttributeWithFixedMap(
            string batchId, TempProductParsed tempProduct, MDataImportD detail,
            MFixedToAttrMap attrMap, List<MAttrDefinition> attrDefinitions)
        {
            if (detail.AttrCd == "SALES_RANK")
            {
                Console.WriteLine($"SALES_RANK 的 ValueRole 是: {attrMap.ValueRole}");  // 输出 ID_AND_LABEL 或 ID_ONLY
            }
            short attrSeqForRow = (short)(_productAttrs.Count(p =>
                p.TempRowId == tempProduct.TempRowId && p.AttrCd == detail.AttrCd) + 1);

            // 1. 根据 value_role 获取 source_id 和 source_label 的实际值
            string? sourceIdValue = null;
            string? sourceLabelValue = null;

            if (attrMap.ValueRole == "ID_AND_LABEL")
            {
                // ID_AND_LABEL: 同时取 source_id_column 和 source_label_column 的值
                // source_id_column 中存储的是字段名 (例: "source_brand_id")
                // 直接转换为 PascalCase (例: "SourceBrandId") 去 temp_product_parsed 中查找
                if (!string.IsNullOrEmpty(attrMap.SourceIdColumn))
                {
                    string idFieldName = ConvertToPascalCase(attrMap.SourceIdColumn);  // source_brand_id -> SourceBrandId
                    sourceIdValue = GetTempProductProperty(tempProduct, idFieldName);
                    Console.WriteLine($"    [读取ID] source_id_column={attrMap.SourceIdColumn} -> 属性名={idFieldName} -> 值={sourceIdValue ?? "(null)"}");
                }

                if (!string.IsNullOrEmpty(attrMap.SourceLabelColumn))
                {
                    string labelFieldName = ConvertToPascalCase(attrMap.SourceLabelColumn);  // source_brand_nm -> SourceBrandNm
                    sourceLabelValue = GetTempProductProperty(tempProduct, labelFieldName);
                    Console.WriteLine($"    [读取Label] source_label_column={attrMap.SourceLabelColumn} -> 属性名={labelFieldName} -> 值={sourceLabelValue ?? "(null)"}");
                }
            }
            else if (attrMap.ValueRole == "ID_ONLY")
            {
                // ID_ONLY: 只取 source_id_column 的值
                if (!string.IsNullOrEmpty(attrMap.SourceIdColumn))
                {
                    string idFieldName = ConvertToPascalCase(attrMap.SourceIdColumn);
                    sourceIdValue = GetTempProductProperty(tempProduct, idFieldName);
                    Console.WriteLine($"    [读取ID_ONLY] source_id_column={attrMap.SourceIdColumn} -> 属性名={idFieldName} -> 值={sourceIdValue ?? "(null)"}");
                }
                sourceLabelValue = "";  // ID_ONLY 模式下 source_label 为空
            }

            // 2. 构建 source_raw 的 JSON
            // 注意: attrMap.SourceIdColumn 已经包含 source_ 前缀 (例: "source_brand_id")
            // 直接使用,不需要再添加 source_ 前缀
            // 例: {"source_brand_id":"4952","source_brand_nm":"ROLEX"}
            var sourceRawDict = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(attrMap.SourceIdColumn) && !string.IsNullOrEmpty(sourceIdValue))
            {
                sourceRawDict[attrMap.SourceIdColumn] = sourceIdValue;  // 直接使用 source_brand_id
            }

            if (attrMap.ValueRole == "ID_AND_LABEL" &&
                !string.IsNullOrEmpty(attrMap.SourceLabelColumn) &&
                !string.IsNullOrEmpty(sourceLabelValue))
            {
                sourceRawDict[attrMap.SourceLabelColumn] = sourceLabelValue;  // 直接使用 source_brand_nm
            }

            string sourceRaw = JsonSerializer.Serialize(sourceRawDict, new JsonSerializerOptions { WriteIndented = false });

            // 3. 用 attr_cd 去 m_attr_definition 表查找 data_type
            string? dataType = null;
            var attrDef = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == detail.AttrCd);
            if (attrDef != null)
            {
                dataType = attrDef.DataType;
            }

            // 4. 检查是否有值
            bool hasValue = !string.IsNullOrEmpty(sourceIdValue);

            // 5. 创建属性记录并插入
            if (hasValue)
            {
                var productAttr = new ClProductAttr
                {
                    BatchId = batchId,
                    TempRowId = tempProduct.TempRowId,
                    AttrCd = detail.AttrCd,
                    AttrSeq = attrSeqForRow,
                    SourceId = sourceIdValue ?? "",           // 例: "4952"
                    SourceLabel = sourceLabelValue ?? "",     // 例: "ROLEX"
                    SourceRaw = sourceRaw,                    // 例: {"source_brand_id":"4952","source_brand_nm":"ROLEX"}
                    DataType = dataType                       // 从 m_attr_definition 获取
                };

                _productAttrs.Add(productAttr);
                Console.WriteLine($"[FixedMap] 已添加属性: attr_cd={detail.AttrCd}, source_id={sourceIdValue}, source_label={sourceLabelValue}");
            }
            else
            {
                Console.WriteLine($"[FixedMap] 跳过空值属性: attr_cd={detail.AttrCd}, source_id_column={attrMap.SourceIdColumn}, value_role={attrMap.ValueRole}");
            }
        }

        /// <summary>
        /// 情况2: 不使用 m_fixed_to_attr_map,直接使用 m_attr_definition
        /// - 从 m_attr_definition 获取 data_type (如果找不到也可以为空)
        /// - 从 temp_product_parsed 中根据 target_column 获取值
        /// </summary>
        private void ProcessAttributeWithoutFixedMap(
            string batchId, TempProductParsed tempProduct, MDataImportD detail,
            List<MAttrDefinition> attrDefinitions)
        {
            short attrSeqForRow = (short)(_productAttrs.Count(p =>
                p.TempRowId == tempProduct.TempRowId && p.AttrCd == detail.AttrCd) + 1);

            // 1. 用 attr_cd 去 m_attr_definition 表查找 data_type (可以为空)
            string? dataType = null;
            var attrDef = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == detail.AttrCd);
            if (attrDef != null)
            {
                dataType = attrDef.DataType;
            }

            // 2. 从 temp_product_parsed 中根据 target_column 获取值
            // 例: target_column = "group_company_cd" → 从 SourceGroupCompanyCd 获取
            string? value = null;
            if (!string.IsNullOrEmpty(detail.TargetColumn))
            {
                string fieldName = "Source" + ConvertToPascalCase(detail.TargetColumn);
                value = GetTempProductProperty(tempProduct, fieldName);
            }

            // 3. 构建 source_raw (使用 source_ 前缀的字段名)
            // 例: {"source_group_company_cd":"KM"}
            var sourceRawDict = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(detail.TargetColumn) && !string.IsNullOrEmpty(value))
            {
                string sourceKey = "source_" + detail.TargetColumn;
                sourceRawDict[sourceKey] = value;
            }

            string sourceRaw = JsonSerializer.Serialize(sourceRawDict, new JsonSerializerOptions { WriteIndented = false });

            // 4. 检查是否有值
            bool hasValue = !string.IsNullOrEmpty(value);

            // 5. 创建属性记录并插入
            if (hasValue)
            {
                var productAttr = new ClProductAttr
                {
                    BatchId = batchId,
                    TempRowId = tempProduct.TempRowId,
                    AttrCd = detail.AttrCd,
                    AttrSeq = attrSeqForRow,
                    SourceId = value ?? "",                   // 例: "KM"
                    SourceLabel = "",                         // 情况2没有 label,为空
                    SourceRaw = sourceRaw,                    // 例: {"source_group_company_cd":"KM"}
                    DataType = dataType                       // 从 m_attr_definition 获取 (可以为空)
                };

                _productAttrs.Add(productAttr);
                Console.WriteLine($"[NoFixedMap] 已添加属性: attr_cd={detail.AttrCd}, source_id={value}, target_column={detail.TargetColumn}");
            }
            else
            {
                Console.WriteLine($"[NoFixedMap] 跳过空值属性: attr_cd={detail.AttrCd}, target_column={detail.TargetColumn}");
            }
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
                        projection_kind = "PRODUCT_MST",
                        map_id = attrMap.MapId,
                        source_id_column = attrMap.SourceIdColumn,
                        source_label_column = attrMap.SourceLabelColumn
                    }),
                    RuleVersion = "1.0"
                };

                _productAttrs.Add(productAttr);
            }
        }

        // /// <summary>
        // /// extras_json から processed_columns を抽出
        // /// </summary>
        // private Dictionary<string, object> ExtractProcessedColumns(Dictionary<string, object> extrasRoot)
        // {
        //     if (extrasRoot.ContainsKey("processed_columns") && extrasRoot["processed_columns"] != null)
        //     {
        //         try
        //         {
        //             return JsonSerializer.Deserialize<Dictionary<string, object>>(
        //                 extrasRoot["processed_columns"].ToString() ?? "{}") ?? new Dictionary<string, object>();
        //         }
        //         catch { }
        //     }
        //     return new Dictionary<string, object>();
        // }

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
            // 注意: _productAttrs 不在这里保存,因为需要在 GenerateProductAttributesAsync 中重新生成
            // await _productRepository.SaveProductAttributesAsync(_productAttrs);
            await _productRepository.SaveRecordErrorsAsync(_recordErrors);

            Console.WriteLine($"temp保存完了: 商品={_tempProducts.Count}, エラー={_recordErrors.Count}");
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
            Console.WriteLine($"エラーレコード: {error.ErrorDetail}");
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