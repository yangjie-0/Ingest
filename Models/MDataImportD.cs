namespace ProductDataIngestion.Models
{
    public class MDataImportD
    {
        public long ProfileId { get; set; }
        public int ColumnSeq { get; set; }
        public string ProjectionKind { get; set; } = string.Empty;
        public string AttrCd { get; set; } = string.Empty;
        public string TargetColumn { get; set; } = string.Empty;
        public string CastType { get; set; } = string.Empty;        // 新增
        public string TransformExpr { get; set; } = string.Empty;   // 新增
        public bool IsRequired { get; set; }
    }
}