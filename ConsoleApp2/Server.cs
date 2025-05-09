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

        try
        {
            while (true)
            {
                string query = reader.ReadLine();
                if (string.IsNullOrEmpty(query) || query.ToLower() == "exit")
                    break;

                // Set previously selected DB
                if (!string.IsNullOrEmpty(selectedDatabase) && dbms.Databases.ContainsKey(selectedDatabase))
                    qp.SetDatabase(selectedDatabase);

                var output = new StringBuilder();
                var consoleWriter = new StringWriter(output);
                Console.SetOut(consoleWriter);
                qp.ExecuteQuery(query);
                consoleWriter.Flush(); // ✅ Flush captured output
                Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });


                // Remember selected DB
                selectedDatabase = qp.GetQPDatabase()?.Name;

                writer.WriteLine(output.ToString().Replace("\n", "\\n")); // Avoid line-breaking issues
                writer.WriteLine("__END__");
            }
        }
        catch (Exception ex)
        {
            writer.WriteLine($"❌ Error: {ex.Message}");
        }
        finally
        {
            client.Close();
            BinaryStorageManager.SaveDBMS(dbms);
        }
    }
}
