using ConsoleApp1;
using System.Text.Json;
using System.IO;

class StorageManager
{
    private const string RootDataPath = "data";

    public static void SaveDBMS(DBMS dbms)
    {
        Directory.CreateDirectory(RootDataPath);

        // Configure JsonSerializerOptions to handle circular references
        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.Preserve // This is the key fix
        };

        foreach (var dbEntry in dbms.Databases)
        {
            string dbPath = Path.Combine(RootDataPath, dbEntry.Key);
            Directory.CreateDirectory(dbPath);

            foreach (var table in dbEntry.Value.Tables)
            {
                string json = JsonSerializer.Serialize(table.Value, options);
                File.WriteAllText(Path.Combine(dbPath, $"{table.Key}.json"), json);
            }
        }

        Console.WriteLine("DBMS saved successfully.");
    }

    public static DBMS LoadDBMS()
    {
        DBMS dbms = new DBMS();

        if (Directory.Exists(RootDataPath))
        {
            foreach (var dbFolder in Directory.GetDirectories(RootDataPath))
            {
                string dbName = Path.GetFileName(dbFolder);
                Database db = new Database(dbName);

                foreach (var file in Directory.GetFiles(dbFolder, "*.json"))
                {
                    string json = File.ReadAllText(file);

                    // Use the same options for deserialization to handle cycles
                    var options = new JsonSerializerOptions
                    {
                        ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.Preserve
                    };

                    Table table = JsonSerializer.Deserialize<Table>(json, options);

                    // Set table parent
                    table.SetParentDatabase(db);

                    // Set column parents
                    foreach (var column in table.Columns)
                    {
                        column.SetParentTable(table);
                    }

                    db.Tables[table.Name] = table;

                    Console.WriteLine($"Loaded table '{table.Name}' into database '{dbName}'.");
                }

                dbms.Databases[dbName] = db;
            }
        }

        Console.WriteLine("DBMS loaded successfully.");
        return dbms;
    }
}
