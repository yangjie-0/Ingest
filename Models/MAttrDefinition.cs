namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 属性定义表 (m_attr_definition)
    /// 定义所有可用的属性代码及其元数据
    /// </summary>
    public class MAttrDefinition
    {
        public long AttrId { get; set; }
        public string AttrCd { get; set; } = string.Empty;
        public string AttrNm { get; set; } = string.Empty;
        public short? AttrSortNo { get; set; }
        public string? GCategoryCd { get; set; }
        public string DataType { get; set; } = string.Empty; // TEXT, NUM, DATE, LIST, BOOL, REF
        public string? GListGroupCd { get; set; }
        public string? SelectType { get; set; } // SINGLE, MULTI
        public bool? IsGoldenAttr { get; set; }
        public short? CleansePhase { get; set; }
        public string[]? RequiredContextKeys { get; set; }
        public string? TargetTable { get; set; }
        public string? TargetColumn { get; set; }
        public string? ProductUnitCd { get; set; } // cm, mm, ct, g
        public bool? CreditActiveFlag { get; set; }
        public string? Usage { get; set; } // PRODUCT, CATALOG, NULL
        public string? TableTypeCd { get; set; } // MST, EAV
        public bool IsGoldenProduct { get; set; }
        public bool IsGoldenAttrEav { get; set; }
        public bool IsActive { get; set; }
        public string? AttrRemarks { get; set; }
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
    }
}
