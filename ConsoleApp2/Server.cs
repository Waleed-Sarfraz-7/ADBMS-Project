using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using ConsoleApp1;

class Server
{
    private static DBMS dbms = new DBMS(); // Shared DBMS
    private static ConcurrencyControl cc = new();
    private static TransactionManager tm = new(cc);

    public static void Main(string[] args)
    {
        dbms = BinaryStorageManager.LoadDBMS();
        TcpListener listener = new TcpListener(IPAddress.Any, 9999);
        listener.Start();
        Console.WriteLine("🚀 Server started at port 9999...");

        while (true)
        {
            var client = listener.AcceptTcpClient();
            Console.WriteLine("✅ Client connected.");

            // Start handling the client in a background thread
            Thread thread = new Thread(() => HandleClient(client));
            thread.Start();
        }
    }

    private static void HandleClient(TcpClient client)
    {
        NetworkStream stream = client.GetStream();
        StreamReader reader = new StreamReader(stream);
        StreamWriter writer = new StreamWriter(stream) { AutoFlush = true };

        var qp = new QueryProcessor(tm, dbms);
        string? selectedDatabase = null;
        //Random rand = new Random();
        //string[] sampleNames = new[] { "Ali", "Bob", "Charlie", "Diana", "Eva", "Frank", "Grace", "Hassan" };
        //int targetCount = 50000;
        //int startingId = 7; // since your XML ends at id=6
        //var database = dbms.Databases["School"];
        //for (int i = startingId; i <= targetCount; i++)
        //{
        //    var id = i.ToString();
        //    var name = sampleNames[rand.Next(sampleNames.Length)]; // ensures repeats
        //    var dept_id = rand.Next(103, 105).ToString();

        //    var row = new Dictionary<string, string>
        //{
        //    { "id", id },
        //    { "name", name },
        //    { "dept_id", dept_id }
        //};

        //    database.Tables["Students"].InsertRow(row); // or use whatever row-adding method you have
        //}


        try
        {
            // ✅ Send available databases to client upon connection
            writer.WriteLine("📂 Available databases:");
            foreach (var dbName in dbms.Databases.Keys)
            {
                writer.WriteLine($"- {dbName}");
            }
            writer.WriteLine("👉 Use `USE database_name;` to select one.");
            writer.WriteLine("__END__");

            while (true)
            {
                try
                {
                    string query = reader.ReadLine();
                    if (string.IsNullOrEmpty(query) || query.ToLower() == "exit")
                        break;

                    // Check if a database is selected
                    if (string.IsNullOrEmpty(selectedDatabase))
                    {
                        // Try detecting USE command
                        if (query.TrimStart().StartsWith("USE", StringComparison.OrdinalIgnoreCase))
                        {
                            // Let it process normally to set selected DB
                        }
                        else
                        {
                            writer.WriteLine("❌ You must select a database first using `USE database_name;`.");
                            writer.WriteLine("__END__");
                            continue;
                        }
                    }

                    // Set DB in QP context
                    if (!string.IsNullOrEmpty(selectedDatabase) && dbms.Databases.ContainsKey(selectedDatabase))
                        qp.SetDatabase(selectedDatabase);

                    var output = new StringBuilder();
                    var consoleWriter = new StringWriter(output);
                    Console.SetOut(consoleWriter);

                    qp.ExecuteQuery(query);

                    consoleWriter.Flush();
                    Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });

                    selectedDatabase = qp.GetQPDatabase()?.Name;

                    writer.WriteLine(output.ToString().Replace("\n", "\\n"));
                    writer.WriteLine("__END__");

                    if (!tm.AnyActiveTransaction())
                    {
                        BinaryStorageManager.SaveDBMS(dbms);
                    }
                }
                catch (Exception ex)
                {
                    writer.WriteLine($"❌ Error: {ex.Message}");
                    writer.WriteLine("__END__");
                    Console.WriteLine($"Error processing query: {ex.Message}");
                    Console.WriteLine($"StackTrace: {ex.StackTrace}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal error: {ex.Message}");
            writer.WriteLine($"❌ Fatal error: {ex.Message}");
            writer.WriteLine("__END__");
        }
        finally
        {
            try
            {
                BinaryStorageManager.SaveDBMS(dbms);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving DBMS state: {ex.Message}");
            }

            client.Close();
            Console.WriteLine("Client connection closed.");
        }
    }


}
