using Npgsql;
using Dapper;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories
{
    // 商品データのリポジトリ実装
    public class ProductRepository : IProductRepository
    {
        private readonly string _connectionString;

        public ProductRepository(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        // 一時商品データを一括保存
        public async Task SaveTempProductsAsync(List<TempProductParsed> products)
        {
            if (products.Count == 0) return;

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO temp_product_parsed (
                    temp_row_id, batch_id, line_no, source_group_company_cd,
                    source_product_cd, source_product_management_cd,
                    source_brand_id, source_brand_nm,
                    source_category_1_id, source_category_1_nm,
                    source_category_2_id, source_category_2_nm,
                    source_category_3_id, source_category_3_nm,
                    source_product_status_cd, source_product_status_nm,
                    source_new_used_kbn, source_quantity,
                    source_stock_existence_cd, source_stock_existence_nm,
                    source_sale_permission_cd, source_sale_permission_nm,
                    source_transfer_status, source_repair_status,
                    source_reservation_status, source_consignment_status,
                    source_accept_status, source_ec_listing_kbn,
                    source_assessment_price_excl_tax, source_assessment_price_incl_tax,
                    source_assessment_tax_rate, source_purchase_price_excl_tax,
                    source_purchase_price_incl_tax, source_purchase_tax_rate,
                    source_display_price_excl_tax, source_display_price_incl_tax,
                    source_display_tax_rate, source_sales_price_excl_tax,
                    source_sales_price_incl_tax, source_sales_tax_rate,
                    source_purchase_rank, source_purchase_rank_name,
                    source_sales_rank, source_sales_rank_name,
                    source_sales_channel_nm, source_sales_channel_region,
                    source_sales_channel_method, source_sales_channel_target,
                    source_purchase_channel_nm, source_purchase_channel_region,
                    source_purchase_channel_method, source_purchase_channel_target,
                    source_store_id, source_store_nm,
                    source_consignor_group_company_id, source_consignor_product_cd,
                    extras_json, step_status
                ) VALUES (
                    @TempRowId, @BatchId, @LineNo, @SourceGroupCompanyCd,
                    @SourceProductCd, @SourceProductManagementCd,
                    @SourceBrandId, @SourceBrandNm,
                    @SourceCategory1Id, @SourceCategory1Nm,
                    @SourceCategory2Id, @SourceCategory2Nm,
                    @SourceCategory3Id, @SourceCategory3Nm,
                    @SourceProductStatusCd, @SourceProductStatusNm,
                    @SourceNewUsedKbn, @SourceQuantity,
                    @SourceStockExistenceCd, @SourceStockExistenceNm,
                    @SourceSalePermissionCd, @SourceSalePermissionNm,
                    @SourceTransferStatus, @SourceRepairStatus,
                    @SourceReservationStatus, @SourceConsignmentStatus,
                    @SourceAcceptStatus, @SourceEcListingKbn,
                    @SourceAssessmentPriceExclTax, @SourceAssessmentPriceInclTax,
                    @SourceAssessmentTaxRate, @SourcePurchasePriceExclTax,
                    @SourcePurchasePriceInclTax, @SourcePurchaseTaxRate,
                    @SourceDisplayPriceExclTax, @SourceDisplayPriceInclTax,
                    @SourceDisplayTaxRate, @SourceSalesPriceExclTax,
                    @SourceSalesPriceInclTax, @SourceSalesTaxRate,
                    @SourcePurchaseRank, @SourcePurchaseRankName,
                    @SourceSalesRank, @SourceSalesRankName,
                    @SourceSalesChannelNm, @SourceSalesChannelRegion,
                    @SourceSalesChannelMethod, @SourceSalesChannelTarget,
                    @SourcePurchaseChannelNm, @SourcePurchaseChannelRegion,
                    @SourcePurchaseChannelMethod, @SourcePurchaseChannelTarget,
                    @SourceStoreId, @SourceStoreNm,
                    @SourceConsignorGroupCompanyId, @SourceConsignorProductCd,
                    @ExtrasJson::jsonb, @StepStatus
                ) ON CONFLICT (temp_row_id) DO NOTHING";

            await connection.ExecuteAsync(sql, products);
        }

        // 商品属性データを一括保存
        public async Task SaveProductAttributesAsync(List<ClProductAttr> attributes)
        {
            if (attributes.Count == 0) return;

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO cl_product_attr (
                    batch_id, temp_row_id, attr_cd, attr_seq,
                    source_id, source_label, source_raw, value_text,
                    value_num, value_date, value_cd, g_list_item_id,
                    data_type, quality_flag, quality_detail_json, provenance_json,
                    rule_version, cre_at, upd_at
                ) VALUES (
                    @BatchId, @TempRowId, @AttrCd, @AttrSeq,
                    @SourceId, @SourceLabel, @SourceRaw, @ValueText,
                    @ValueNum, @ValueDate, @ValueCd, @GListItemId,
                    @DataType, @QualityFlag, @QualityDetailJson::jsonb, @ProvenanceJson::jsonb,
                    @RuleVersion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                ) ON CONFLICT (batch_id, temp_row_id, attr_cd, attr_seq) DO NOTHING";

            await connection.ExecuteAsync(sql, attributes);
        }

        // エラーレコードを一括保存
        public async Task SaveRecordErrorsAsync(List<RecordError> errors)
        {
            if (errors.Count == 0) return;

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            // record_error テーブルの主キーは batch_id のみなので、
            // 複数エラーを保存するためにはテーブル構造を変更するか、
            // 最初のエラーのみを保存する必要があります
            // ここでは最初のエラーのみを保存します
            var firstError = errors.First();

            var sql = @"
                INSERT INTO record_error (
                    batch_id, step, record_ref, error_cd, error_detail, raw_fragment,
                    cre_at, upd_at
                ) VALUES (
                    @BatchId, @Step, @RecordRef, @ErrorCd, @ErrorDetail, @RawFragment,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                ) ON CONFLICT (batch_id) DO UPDATE SET
                    step = EXCLUDED.step,
                    record_ref = record_error.record_ref || '; ' || EXCLUDED.record_ref,
                    error_cd = EXCLUDED.error_cd,
                    error_detail = record_error.error_detail || E'\n' || EXCLUDED.error_detail,
                    raw_fragment = EXCLUDED.raw_fragment,
                    upd_at = CURRENT_TIMESTAMP";

            await connection.ExecuteAsync(sql, errors);
        }
    }
}
