namespace ProductDataIngestion.Models
{
    public class TempProductParsed
    {
        public Guid TempRowId { get; set; }
        public string BatchId { get; set; } = string.Empty;
        public long LineNo { get; set; }
        public string SourceGroupCompanyCd { get; set; } = string.Empty;
        public string? SourceProductCd { get; set; }
        public string? SourceBrandId { get; set; }
        public string? SourceBrandNm { get; set; }
        public string? SourceCategory1Id { get; set; }
        public string ExtrasJson { get; set; } = "{}";
    }
}