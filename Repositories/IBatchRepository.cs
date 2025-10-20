using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories
{
    // バッチ実行情報のリポジトリインターフェース
    public interface IBatchRepository
    {
        // バッチ実行情報を作成
        Task CreateBatchRunAsync(BatchRun batchRun);

        // バッチ実行情報を更新
        Task UpdateBatchRunAsync(BatchRun batchRun);

        // バッチIDでバッチ実行情報を取得
        Task<BatchRun?> GetBatchRunByIdAsync(string batchId);
    }
}
