using System;
using System.Collections.Generic;

namespace ProductDataIngestion.Models
{
    public class ClProductAttr
    {
        public string BatchId { get; set; }
        public Guid TempRowId { get; set; }
        public string AttrCd { get; set; }
        public short AttrSeq { get; set; }
        public string SourceId { get; set; }
        public string SourceLabel { get; set; }
        public string SourceRaw { get; set; }
        public string ValueText { get; set; }
        public decimal? ValueNum { get; set; }
        public DateTime? ValueDate { get; set; }
        public string ValueCd { get; set; }
        public long? GListItemId { get; set; }
        public string DataType { get; set; }
        public string QualityFlag { get; set; } = "OK";
        public Dictionary<string, object> QualityDetailJson { get; set; } = new Dictionary<string, object>();
        public Dictionary<string, object> ProvenanceJson { get; set; } = new Dictionary<string, object>();
        public string RuleVersion { get; set; }
        public DateTime CreAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;
    }
}