namespace ProductDataIngestion.Models
{
    /// <summary>
    /// クレンジング結果商品属性テーブル (cl_product_attr)
    /// INGEST 段階：source_raw のみ設定、value_* と data_type は未設定（null）
    /// CLEANSE 段階：value_* と data_type を設定
    /// </summary>
    public class ClProductAttr
    {
        public string BatchId { get; set; } = string.Empty;
        public Guid TempRowId { get; set; }
        public string AttrCd { get; set; } = string.Empty;
        public short AttrSeq { get; set; }
        public string? SourceId { get; set; }
        public string? SourceLabel { get; set; }
        public string SourceRaw { get; set; } = string.Empty;
        public string? ValueText { get; set; }  // INGEST では null、CLEANSE で設定
        public decimal? ValueNum { get; set; }
        public DateTime? ValueDate { get; set; }
        public string? ValueCd { get; set; }
        public long? GListItemId { get; set; }
        public string? DataType { get; set; }  // INGEST では null、CLEANSE で設定
        public string QualityFlag { get; set; } = "OK";
        public string QualityDetailJson { get; set; } = "{}";
        public string ProvenanceJson { get; set; } = "{}";
        public string RuleVersion { get; set; } = string.Empty;
        public DateTime? CreAt { get; set; }  // 由数据库 CURRENT_TIMESTAMP 自动设置
        public DateTime? UpdAt { get; set; }  // 由数据库 CURRENT_TIMESTAMP 自动设置
    }
}