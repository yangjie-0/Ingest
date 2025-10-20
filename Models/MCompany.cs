using System;
namespace ProductDataIngestion.Models
{
    public class MCompany
    {
        public string GroupCompanyId { get; set; } = string.Empty;
        public string GroupCompanyCd { get; set; } = string.Empty;
        public string GroupCompanyNm { get; set; } = string.Empty;
        public string DefaultCurrencyCd { get; set; } = string.Empty;
        public bool IsActive { get; set; }
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(GroupCompanyCd) && 
                   !string.IsNullOrWhiteSpace(GroupCompanyNm) && 
                   IsActive;
        }
    }
}