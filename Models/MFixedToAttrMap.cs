using System;

namespace ProductDataIngestion.Models
{
    public class MFixedToAttrMap
    {
        public long MapId { get; set; }
        public string GroupCompanyCd { get; set; }
        public string DataKind { get; set; }
        public string AttrCd { get; set; }
        public string SourceIdColumn { get; set; }
        public string SourceLabelColumn { get; set; }
        public string ValueRole { get; set; }
        public string DataTypeOverride { get; set; }
        public string SplitMode { get; set; }
        public bool IsActive { get; set; } = true;
        public int Priority { get; set; } = 100;
        public string FixedRemarks { get; set; }
        public DateTime CreAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;
    }
}