using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProductDataIngestion.Models
{
    public class MDataImportSetting
    {
        public long ProfileId { get; set; }
        public string UsageNm { get; set; } = string.Empty;
        public string GroupCompanyCd { get; set; } = string.Empty;
        public string TargetEntity { get; set; } = string.Empty;
        public string CharacterCd { get; set; } = "UTF-8";
        public string Delimiter { get; set; } = ",";
        public long HeaderRowIndex { get; set; } = 1;
        public string SkipRows { get; set; } = string.Empty; // 跳过行号，用逗号分隔
        public bool IsActive { get; set; } = true;
    }
}