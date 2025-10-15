namespace ProductDataIngestion.Models
{
    public class RecordError
    {
        public string BatchId { get; set; } = string.Empty;
        public string Step { get; set; } = string.Empty;
        public string RecordRef { get; set; } = string.Empty;
        public string ErrorCd { get; set; } = string.Empty;
        public string ErrorDetail { get; set; } = string.Empty;
        public string RawFragment { get; set; } = string.Empty;
    }
}