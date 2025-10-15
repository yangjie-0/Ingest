using System;
using System.Collections.Generic;

namespace ProductDataIngestion.Models
{
    public class IngestManifest
    {
        public string BatchId { get; set; }
        public string S3Bucket { get; set; }
        public string ObjectKey { get; set; }
        public string Etag { get; set; }
        public long ObjectSize { get; set; }
        public int RowCount { get; set; }
        public Dictionary<string, object> MetaJson { get; set; } = new Dictionary<string, object>();
        public DateTime CreAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;
    }
}