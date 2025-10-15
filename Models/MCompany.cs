using System;

namespace ProductDataIngestion.Models
{
    public class MCompany
    {
        public long GroupCompanyId { get; set; }
        public string GroupCompanyCd { get; set; }
        public string GroupCompanyNm { get; set; }
        public string DefaultCurrencyCd { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime CreAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;
    }
}