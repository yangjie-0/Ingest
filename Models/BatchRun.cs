namespace ProductDataIngestion.Models
{
    public class BatchRun
    {
        public string BatchId { get; set; } = string.Empty;
        public string IdemKey { get; set; } = string.Empty;
        public string GroupCompanyCd { get; set; } = string.Empty;
        public string DataKind { get; set; } = string.Empty;
        public string FileKey { get; set; } = string.Empty;
        public string BatchStatus { get; set; } = string.Empty;
        public DateTime StartedAt { get; set; }
        public DateTime? EndedAt { get; set; }
        public string CountsJson { get; set; } = string.Empty;
    }
}