namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 属性定义表 (m_attr_definition)
    /// 定义所有可用的属性代码及其元数据
    /// </summary>
    public class MAttrDefinition
    {
        public long AttrDefId { get; set; }
        public string GroupCompanyCd { get; set; } = string.Empty;
        public string AttrCd { get; set; } = string.Empty;
        public string AttrNm { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty; // TEXT, NUM, DATE, LIST, BOOL, REF
        public string? DefaultValue { get; set; }
        public bool IsRequired { get; set; }
        public bool IsMultiValue { get; set; }
        public string? ValidationRule { get; set; }
        public string? AttrRemarks { get; set; }
        public bool IsActive { get; set; }
        public int DisplayOrder { get; set; }
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
    }
}
