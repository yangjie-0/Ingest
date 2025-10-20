using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories
{
    // 商品データのリポジトリインターフェース
    public interface IProductRepository
    {
        // 一時商品データを一括保存
        Task SaveTempProductsAsync(List<TempProductParsed> products);

        // 商品属性データを一括保存
        Task SaveProductAttributesAsync(List<ClProductAttr> attributes);

        // エラーレコードを一括保存
        Task SaveRecordErrorsAsync(List<RecordError> errors);
    }
}
