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
                string tablePath = Path.Combine(dbPath, table.Key);
                Directory.CreateDirectory(tablePath);

                // Save the table data
                string tableJson = JsonSerializer.Serialize(table.Value, options);
                File.WriteAllText(Path.Combine(tablePath, $"{table.Key}.json"), tableJson);

                // Save the indexes for the table
                if (table.Value.Indexes != null && table.Value.Indexes.Count > 0)
                {
                    string indexesPath = Path.Combine(tablePath, "indexes");
                    Directory.CreateDirectory(indexesPath);

                    foreach (var index in table.Value.Indexes)
                    {
                        // Serialize the index (BTree)
                        string indexJson = JsonSerializer.Serialize(index.Value, options);
                        File.WriteAllText(Path.Combine(indexesPath, $"{index.Key}.json"), indexJson);
                    }
                }

                Console.WriteLine($"Saved table '{table.Key}' with indexes into database '{dbEntry.Key}'.");
            }
        }

        Console.WriteLine("DBMS saved successfully.");
    }

    public static DBMS LoadDBMS()
    {
        DBMS dbms = new DBMS();

        if (Directory.Exists(RootDataPath))
        {
            // Traverse through each database directory
            foreach (var dbFolder in Directory.GetDirectories(RootDataPath))
            {
                string dbName = Path.GetFileName(dbFolder);
                Database db = new Database(dbName);

                // Traverse through each table directory inside the current database
                foreach (var tableFolder in Directory.GetDirectories(dbFolder))
                {
                    string tableName = Path.GetFileName(tableFolder);
                    string tableJsonFile = Path.Combine(tableFolder, $"{tableName}.json");

                    if (File.Exists(tableJsonFile))
                    {
                        // Read the table data JSON
                        string tableJson = File.ReadAllText(tableJsonFile);
                        var options = new JsonSerializerOptions
                        {
                            ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.Preserve
                        };

                        // Deserialize the table object
                        Table table = JsonSerializer.Deserialize<Table>(tableJson, options);

                        // Set parent relationships for the table and columns
                        table.SetParentDatabase(db);
                        foreach (var column in table.Columns)
                        {
                            column.SetParentTable(table);
                        }

                        // Load indexes for the table
                        LoadIndexesForTable(table, tableFolder);

                        // Add the table to the database
                        db.Tables[table.Name] = table;

                        Console.WriteLine($"Loaded table '{table.Name}' into database '{dbName}'.");
                    }
                }

                // Add the database to the DBMS
                dbms.Databases[dbName] = db;
                Console.WriteLine($"Loaded database '{dbName}' into DBMS.");
            }
        }

        Console.WriteLine("DBMS loaded successfully.");
        return dbms;
    }

    private static void LoadIndexesForTable(Table table, string tableFolder)
    {
        string indexesFolder = Path.Combine(tableFolder, "indexes");

        // Check if the indexes folder exists
        if (Directory.Exists(indexesFolder))
        {
            foreach (var indexFile in Directory.GetFiles(indexesFolder, "*.json"))
            {
                // Read the index JSON file
                string indexJson = File.ReadAllText(indexFile);

                var options = new JsonSerializerOptions
                {
                    ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.Preserve
                };

                // Deserialize the index object (SerializableBTree)
                SerializableBTree index = JsonSerializer.Deserialize<SerializableBTree>(indexJson, options);

                // Get the index name from the file name (without extension)
                string indexName = Path.GetFileNameWithoutExtension(indexFile);

                // Add the index to the table's index collection
                table.Indexes[indexName] = index;

                Console.WriteLine($"Loaded index '{indexName}' for table '{table.Name}'.");
            }
        }
    }

}
