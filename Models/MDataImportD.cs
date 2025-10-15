namespace ProductDataIngestion.Models
{
    public class MDataImportD
    {
        public long ProfileId { get; set; }
        public int ColumnSeq { get; set; }
        public string TargetEntity { get; set; } = string.Empty;
        public string AttrCd { get; set; } = string.Empty;
        public string TargetColumn { get; set; } = string.Empty;
        public bool IsRequired { get; set; }
    }
}