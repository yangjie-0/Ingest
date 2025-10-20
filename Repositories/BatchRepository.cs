using Npgsql;
using Dapper;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories
{
    // バッチ実行情報のリポジトリ実装
    public class BatchRepository : IBatchRepository
    {
        private readonly string _connectionString;

        public BatchRepository(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        // バッチ実行情報を作成
        public async Task CreateBatchRunAsync(BatchRun batchRun)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO batch_run (
                    batch_id, idem_key, s3_bucket, etag, group_company_cd,
                    data_kind, file_key, batch_status, counts_json,
                    started_at, ended_at, cre_at, upd_at
                ) VALUES (
                    @BatchId, @IdemKey, @S3Bucket, @Etag, @GroupCompanyCd,
                    @DataKind, @FileKey, @BatchStatus, @CountsJson::jsonb,
                    @StartedAt, @EndedAt, @CreAt, @UpdAt
                )";

            await connection.ExecuteAsync(sql, new
            {
                batchRun.BatchId,
                batchRun.IdemKey,
                batchRun.S3Bucket,
                batchRun.Etag,
                batchRun.GroupCompanyCd,
                batchRun.DataKind,
                batchRun.FileKey,
                batchRun.BatchStatus,
                batchRun.CountsJson,
                batchRun.StartedAt,
                batchRun.EndedAt,
                CreAt = DateTime.UtcNow,
                UpdAt = DateTime.UtcNow
            });
        }

        // バッチ実行情報を更新
        public async Task UpdateBatchRunAsync(BatchRun batchRun)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                UPDATE batch_run
                SET batch_status = @BatchStatus,
                    counts_json = @CountsJson::jsonb,
                    ended_at = @EndedAt,
                    upd_at = @UpdAt
                WHERE batch_id = @BatchId";

            await connection.ExecuteAsync(sql, new
            {
                batchRun.BatchId,
                batchRun.BatchStatus,
                batchRun.CountsJson,
                batchRun.EndedAt,
                UpdAt = DateTime.UtcNow
            });
        }

        // バッチIDでバッチ実行情報を取得
        public async Task<BatchRun?> GetBatchRunByIdAsync(string batchId)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                SELECT batch_id as BatchId, idem_key as IdemKey,
                       s3_bucket as S3Bucket, etag as Etag,
                       group_company_cd as GroupCompanyCd, data_kind as DataKind,
                       file_key as FileKey, batch_status as BatchStatus,
                       counts_json as CountsJson, started_at as StartedAt,
                       ended_at as EndedAt, cre_at as CreAt, upd_at as UpdAt
                FROM batch_run
                WHERE batch_id = @BatchId";

            return await connection.QueryFirstOrDefaultAsync<BatchRun>(sql, new { BatchId = batchId });
        }
    }
}
