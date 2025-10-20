public class MDataImportSetting
{
    public long ProfileId { get; set; }
    public string UsageNm { get; set; } = string.Empty;
    public string GroupCompanyCd { get; set; } = string.Empty;
    public string TargetEntity { get; set; } = string.Empty;
    public string? CharacterCd { get; set; }
    public string? Delimiter { get; set; }
    public int HeaderRowIndex { get; set; }
    public int SkipRowCount { get; set; } = 0;  
    public bool IsActive { get; set; }
    public string? ImportSettingRemarks { get; set; }
}