using System.Net.Sockets;
using System.Text;

class Client
{
    public static void Main(string[] args)
    {
        Console.WriteLine("🔌 Connecting to server...");
        using var client = new TcpClient("127.0.0.1", 9999);
        var stream = client.GetStream();
        var reader = new StreamReader(stream);
        var writer = new StreamWriter(stream) { AutoFlush = true };
        try
        {
            while (true)
            {
                Console.Write("SQL> ");
                string query = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(query) || query.ToLower() == "exit")
                {
                    writer.WriteLine("exit");
                    break;
                }

                writer.WriteLine(query);

                StringBuilder fullResponse = new();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line == "__END__") break;
                    fullResponse.AppendLine(line.Replace("\\n", "\n"));
                }
                Console.WriteLine(fullResponse.ToString());

            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }

    }
}
