namespace ProductDataIngestion.Models
{
    public class TempProductParsed
    {
        public Guid TempRowId { get; set; }
        public string BatchId { get; set; } = string.Empty;
        public long LineNo { get; set; }
        public string SourceGroupCompanyCd { get; set; } = string.Empty;
        
        // 基本商品信息
        public string? SourceProductCd { get; set; }
        public string? SourceProductManagementCd { get; set; }
        public string? SourceBrandId { get; set; }
        public string? SourceBrandNm { get; set; }
        
        // 分类信息
        public string? SourceCategory1Id { get; set; }
        public string? SourceCategory1Nm { get; set; }
        public string? SourceCategory2Id { get; set; }
        public string? SourceCategory2Nm { get; set; }
        public string? SourceCategory3Id { get; set; }
        public string? SourceCategory3Nm { get; set; }
        
        // 商品状态
        public string? SourceProductStatusCd { get; set; }
        public string? SourceProductStatusNm { get; set; }
        public string? SourceNewUsedKbn { get; set; }
        public string? SourceQuantity { get; set; }
        
        // 库存信息
        public string? SourceStockExistenceCd { get; set; }
        public string? SourceStockExistenceNm { get; set; }
        public string? SourceSalePermissionCd { get; set; }
        public string? SourceSalePermissionNm { get; set; }
        
        // 状态信息
        public string? SourceTransferStatus { get; set; }
        public string? SourceRepairStatus { get; set; }
        public string? SourceReservationStatus { get; set; }
        public string? SourceConsignmentStatus { get; set; }
        public string? SourceAcceptStatus { get; set; }
        public string? SourceEcListingKbn { get; set; }
        
        // 价格信息
        public string? SourceAssessmentPriceExclTax { get; set; }
        public string? SourceAssessmentPriceInclTax { get; set; }
        public string? SourceAssessmentTaxRate { get; set; }
        public string? SourcePurchasePriceExclTax { get; set; }
        public string? SourcePurchasePriceInclTax { get; set; }
        public string? SourcePurchaseTaxRate { get; set; }
        public string? SourceDisplayPriceExclTax { get; set; }
        public string? SourceDisplayPriceInclTax { get; set; }
        public string? SourceDisplayTaxRate { get; set; }
        public string? SourceSalesPriceExclTax { get; set; }
        public string? SourceSalesPriceInclTax { get; set; }
        public string? SourceSalesTaxRate { get; set; }
        
        // 排名信息
        public string? SourcePurchaseRank { get; set; }
        public string? SourcePurchaseRankNm { get; set; }
        public string? SourceSalesRank { get; set; }
        public string? SourceSalesRankNm { get; set; }
        
        // 渠道信息
        public string? SourceSalesChannelNm { get; set; }
        public string? SourceSalesChannelRegion { get; set; }
        public string? SourceSalesChannelMethod { get; set; }
        public string? SourceSalesChannelTarget { get; set; }
        public string? SourcePurchaseChannelNm { get; set; }
        public string? SourcePurchaseChannelRegion { get; set; }
        public string? SourcePurchaseChannelMethod { get; set; }
        public string? SourcePurchaseChannelTarget { get; set; }
        
        // 店铺信息
        public string? SourceStoreId { get; set; }
        public string? SourceStoreNm { get; set; }
        
        // 委托信息
        public string? SourceConsignorGroupCompanyId { get; set; }
        public string? SourceConsignorProductCd { get; set; }
        
        public string StepStatus { get; set; } = "READY";
        public string ExtrasJson { get; set; } = "{}";
    }
}