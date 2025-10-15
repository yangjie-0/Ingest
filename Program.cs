using ProductDataIngestion.Services;

class Program
{
    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.WriteLine("========================================");
        Console.WriteLine("  商品データ取込システム");
        Console.WriteLine("========================================\n");

        try
        {
            // CSV文件路径(请修改为您的实际文件路径)
            string csvFilePath = @"C:\Users\yang.jie.tw\Documents\ProductDataIngestion\sample_products.csv";
            
            // 检查文件是否存在
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"エラー: ファイルが見つかりません: {csvFilePath}");
                Console.WriteLine("\nサンプルCSVファイルを作成してください:");
                Console.WriteLine("sample_products.csv に以下の内容:");
                Console.WriteLine("GP会社コード,商品コード,ブランドID,ブランド名,カテゴリ1ID");
                Console.WriteLine("KM,0000123456,4952,ROLEX,20");
                Console.WriteLine("KM,0000123457,4953,OMEGA,20");
                return;
            }

            // GP会社コード
            string groupCompanyCd = "KM";

            // 取込サービスのインスタンス作成
            var ingestService = new IngestService();

            // CSV取込処理実行
            string batchId = await ingestService.ProcessCsvFileAsync(csvFilePath, groupCompanyCd);

            // 結果表示
            ingestService.PrintResults();

            Console.WriteLine($"\nバッチID: {batchId}");
            Console.WriteLine("\n処理が完了しました。");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n予期しないエラーが発生しました:");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine($"スタックトレース:\n{ex.StackTrace}");
        }

        Console.WriteLine("\nEnterキーを押して終了してください...");
        Console.ReadLine();
    }
}