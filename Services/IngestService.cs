using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    public class IngestService
    {
        private readonly List<BatchRun> _batchRuns = new();
        private readonly List<TempProductParsed> _tempProducts = new();
        private readonly List<RecordError> _recordErrors = new();

        // 模拟数据库中的导入设置(实际应从数据库读取)
        private MDataImportSetting GetImportSetting(string groupCompanyCd, string targetEntity)
        {
            // 这里返回模拟数据,实际应该从数据库查询
            return new MDataImportSetting
            {
                ProfileId = 1,
                UsageNm = "KM-PRODUCT",
                GroupCompanyCd = groupCompanyCd,
                TargetEntity = targetEntity,
                CharacterCd = "UTF-8",
                Delimiter = ",",
                HeaderRowIndex = 1,  // 表头行号
                SkipRows = "3,6,9",  // 跳过行号，用逗号分隔
                IsActive = true
            };
        }

        // 模拟数据库中的导入详细规则(实际应从数据库读取)
        private List<MDataImportD> GetImportDetails(long profileId)
        {
            // 返回模拟的列映射规则
            // 实际应该从数据库的 m_data_import_d 表查询
            return new List<MDataImportD>
            {
                new() { ProfileId = profileId, ColumnSeq = 1, TargetEntity = "PRODUCT_MST", AttrCd = "GP_CD", TargetColumn = "source_group_company_cd", IsRequired = true },
                new() { ProfileId = profileId, ColumnSeq = 2, TargetEntity = "PRODUCT_MST", AttrCd = "PRODUCT_CD", TargetColumn = "source_product_cd", IsRequired = true },
                new() { ProfileId = profileId, ColumnSeq = 3, TargetEntity = "PRODUCT_MST", AttrCd = "BRAND_ID", TargetColumn = "source_brand_id", IsRequired = false },
                new() { ProfileId = profileId, ColumnSeq = 4, TargetEntity = "PRODUCT_MST", AttrCd = "BRAND_NM", TargetColumn = "source_brand_nm", IsRequired = false },
                new() { ProfileId = profileId, ColumnSeq = 5, TargetEntity = "PRODUCT_MST", AttrCd = "CATEGORY_1_ID", TargetColumn = "source_category_1_id", IsRequired = false },
                // ... 添加更多列映射
            };
        }

        // 模拟EAV属性映射表
        private Dictionary<string, string> GetFixedToAttrMap()
        {
            return new Dictionary<string, string>
            {
                { "COLOR", "color_attr" },
                { "SIZE", "size_attr" },
                { "WEIGHT", "weight_attr" }
                // ... 更多属性映射
            };
        }

        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd)
        {
            Console.WriteLine("=== 开始取込処理 ===");
            Console.WriteLine($"文件路径: {filePath}");
            Console.WriteLine($"GP会社コード: {groupCompanyCd}");
            Console.WriteLine();

            // 1. 生成批次ID
            string batchId = $"BATCH_{DateTime.Now:yyyyMMddHHmmss}_{Guid.NewGuid():N}";
            Console.WriteLine($"生成 BatchId: {batchId}");

            var batchRun = new BatchRun
            {
                BatchId = batchId,
                IdemKey = $"{filePath}_{File.GetLastWriteTime(filePath).Ticks}",
                GroupCompanyCd = groupCompanyCd,
                DataKind = "PRODUCT",
                FileKey = filePath,
                BatchStatus = "RUNNING",
                StartedAt = DateTime.UtcNow,
                CountsJson = "{\"INGEST\":{\"read\":0,\"ok\":0,\"ng\":0}}"
            };
            _batchRuns.Add(batchRun);

            try
            {
                // 2. 获取文件取込规则
                Console.WriteLine("\n--- ステップ2: ファイル取込ルールの取得 ---");
                var importSetting = GetImportSetting(groupCompanyCd, "PRODUCT");
                Console.WriteLine($"ProfileId: {importSetting.ProfileId}");
                Console.WriteLine($"文字コード: {importSetting.CharacterCd}");
                Console.WriteLine($"区切り文字: {importSetting.Delimiter}");
                Console.WriteLine($"ヘッダー行番号: {importSetting.HeaderRowIndex}");
                Console.WriteLine($"スキップ行: {importSetting.SkipRows}");

                var importDetails = GetImportDetails(importSetting.ProfileId);
                Console.WriteLine($"列マッピング数: {importDetails.Count}");

                var attrMap = GetFixedToAttrMap();
                Console.WriteLine($"EAV属性マッピング数: {attrMap.Count}");

                // 3. CSV読み込み前のI/O設定
                Console.WriteLine("\n--- ステップ3: CSV読み込み設定 ---");
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = false, // 手动处理表头
                    Delimiter = importSetting.Delimiter,
                    BadDataFound = null,
                    MissingFieldFound = null,
                    Encoding = Encoding.GetEncoding(importSetting.CharacterCd)
                };

                int readCount = 0;
                int okCount = 0;
                int ngCount = 0;

                using var reader = new StreamReader(filePath, Encoding.GetEncoding(importSetting.CharacterCd));
                using var csv = new CsvReader(reader, config);

                // 4. 处理表头和跳过行
                Console.WriteLine("\n--- ステップ4: ヘッダーとスキップ行の処理 ---");

                string[]? headers = null;
                long currentLine = 0;

                // 读取到表头行
                while (currentLine < importSetting.HeaderRowIndex)
                {
                    if (await csv.ReadAsync())
                    {
                        currentLine++;
                        if (currentLine == importSetting.HeaderRowIndex)
                        {
                            // 获取表头
                            headers = csv.Parser.Record;
                            Console.WriteLine($"ヘッダー行を読み込み (行 {currentLine}):");
                            if (headers != null)
                            {
                                for (int i = 0; i < headers.Length; i++)
                                {
                                    Console.WriteLine($"  列番号 {i + 1}: {headers[i]}");
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine($"行 {currentLine} をスキップ (ヘッダー前)");
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                // 处理需要跳过的行
                var skipRows = new HashSet<long>();
                if (!string.IsNullOrEmpty(importSetting.SkipRows))
                {
                    var skipRowStrings = importSetting.SkipRows.Split(',');
                    foreach (var rowStr in skipRowStrings)
                    {
                        if (long.TryParse(rowStr.Trim(), out long skipRow))
                        {
                            skipRows.Add(skipRow);
                        }
                    }
                    Console.WriteLine($"スキップ対象行: {string.Join(", ", skipRows)}");
                }

                Console.WriteLine($"\n--- ステップ5: データ行読み込み開始 ---");

                // 5. 处理数据行
                while (await csv.ReadAsync())
                {
                    currentLine++;  // 这里currentLine已经指向当前读取的行
                    
                    // 检查是否需要跳过这一行
                    if (skipRows.Contains(currentLine))
                    {
                        Console.WriteLine($"行 {currentLine} をスキップ (設定によるスキップ)");
                        continue;
                    }

                    readCount++;

                    try
                    {
                        var tempProduct = new TempProductParsed
                        {
                            TempRowId = Guid.NewGuid(),
                            BatchId = batchId,
                            LineNo = currentLine,  // 使用正确的行号
                            SourceGroupCompanyCd = groupCompanyCd,
                            ExtrasJson = "{}"
                        };

                        Console.WriteLine($"\n行番号 {currentLine}:");

                        var extrasDict = new Dictionary<string, object>();
                        var eavAttributes = new Dictionary<string, string>();

                        // 根据列映射规则读取并转换数据
                        foreach (var detail in importDetails)
                        {
                            int colIndex = detail.ColumnSeq - 1; // 转换为0-based索引
                            
                            if (colIndex < 0 || colIndex >= (csv.Parser.Record?.Length ?? 0))
                            {
                                Console.WriteLine($"  列{detail.ColumnSeq}: [範囲外]");
                                continue;
                            }

                            string? rawValue = csv.GetField(colIndex);
                            
                            // 应用trim转换
                            string? transformedValue = rawValue?.Trim().Trim('\u3000');

                            Console.WriteLine($"  列{detail.ColumnSeq} ({headers?[colIndex] ?? "N/A"}): \"{transformedValue}\"");

                            // 备份所有原始数据到extras_json
                            extrasDict[$"col_{detail.ColumnSeq}"] = new {
                                header = headers?[colIndex] ?? "N/A",
                                raw_value = rawValue,
                                transformed_value = transformedValue,
                                attr_cd = detail.AttrCd,
                                target_column = detail.TargetColumn
                            };

                            // 根据目标列名设置值
                            if (!string.IsNullOrEmpty(detail.TargetColumn))
                            {
                                SetPropertyValue(tempProduct, detail.TargetColumn, transformedValue);
                            }
                            // 检查attr_cd，处理EAV属性
                            else if (!string.IsNullOrEmpty(detail.AttrCd))
                            {
                                if (attrMap.TryGetValue(detail.AttrCd, out string? mappedAttr))
                                {
                                    eavAttributes[mappedAttr] = transformedValue ?? "";
                                }
                                else
                                {
                                    eavAttributes[detail.AttrCd] = transformedValue ?? "";
                                }
                            }

                            // 必须检查
                            if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                            {
                                throw new Exception($"必須項目が空です: 列{detail.ColumnSeq} ({detail.AttrCd})");
                            }
                        }

                        // 保存EAV属性到extras_json
                        if (eavAttributes.Any())
                        {
                            extrasDict["eav_attributes"] = eavAttributes;
                        }

                        // 更新extras_json
                        tempProduct.ExtrasJson = System.Text.Json.JsonSerializer.Serialize(extrasDict);

                        // 6. tempへの保存
                        _tempProducts.Add(tempProduct);
                        okCount++;
                        Console.WriteLine($"  → 取込成功 (TempRowId: {tempProduct.TempRowId})");
                    }
                    catch (Exception ex)
                    {
                        ngCount++;
                        var error = new RecordError
                        {
                            BatchId = batchId,
                            Step = "INGEST",
                            RecordRef = $"line:{currentLine}",
                            ErrorCd = "PARSE_FAILED",
                            ErrorDetail = ex.Message,
                            RawFragment = csv.Context.Parser.RawRecord ?? ""
                        };
                        _recordErrors.Add(error);
                        Console.WriteLine($"  → エラー: {ex.Message}");
                    }
                }

                // 10. バッチ統計更新
                batchRun.CountsJson = $"{{\"INGEST\":{{\"read\":{readCount},\"ok\":{okCount},\"ng\":{ngCount}}}}}";
                batchRun.BatchStatus = ngCount > 0 ? "PARTIAL" : "SUCCESS";
                batchRun.EndedAt = DateTime.UtcNow;

                Console.WriteLine("\n=== 取込処理完了 ===");
                Console.WriteLine($"読込件数: {readCount}");
                Console.WriteLine($"成功件数: {okCount}");
                Console.WriteLine($"エラー件数: {ngCount}");
                Console.WriteLine($"バッチ状態: {batchRun.BatchStatus}");

                return batchId;
            }
            catch (Exception ex)
            {
                batchRun.BatchStatus = "FAILED";
                batchRun.EndedAt = DateTime.UtcNow;
                Console.WriteLine($"\n!!! 致命的エラー !!!");
                Console.WriteLine($"エラー: {ex.Message}");
                Console.WriteLine($"スタックトレース: {ex.StackTrace}");
                throw;
            }
        }

        // リフレクションを使ってプロパティに値を設定
        private void SetPropertyValue(TempProductParsed obj, string propertyName, string? value)
        {
            var property = typeof(TempProductParsed).GetProperty(
                propertyName,
                System.Reflection.BindingFlags.IgnoreCase | 
                System.Reflection.BindingFlags.Public | 
                System.Reflection.BindingFlags.Instance
            );
            
            property?.SetValue(obj, value);
        }

        // デバッグ用: 取込結果を表示
        public void PrintResults()
        {
            Console.WriteLine("\n=== 取込結果サマリー ===");
            Console.WriteLine($"TempProductParsed件数: {_tempProducts.Count}");
            Console.WriteLine($"RecordError件数: {_recordErrors.Count}");

            if (_tempProducts.Any())
            {
                Console.WriteLine("\n--- 取込データサンプル ---");
                foreach (var product in _tempProducts.Take(3))
                {
                    Console.WriteLine($"LineNo: {product.LineNo}, ProductCD: {product.SourceProductCd}, Extras: {product.ExtrasJson}");
                }
            }

            if (_recordErrors.Any())
            {
                Console.WriteLine("\n--- エラー詳細 ---");
                foreach (var error in _recordErrors)
                {
                    Console.WriteLine($"行: {error.RecordRef}, コード: {error.ErrorCd}, 内容: {error.ErrorDetail}");
                }
            }
        }
    }
}