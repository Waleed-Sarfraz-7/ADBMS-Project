using ConsoleApp1;
using System.Text.Json;

class StorageManager
{
    private const string RootDataPath = "data";

    public static void SaveDBMS(DBMS dbms)
    {
        Directory.CreateDirectory(RootDataPath);
        foreach (var dbEntry in dbms.Databases)
        {
            string dbPath = Path.Combine(RootDataPath, dbEntry.Key);
            Directory.CreateDirectory(dbPath);

            foreach (var table in dbEntry.Value.Tables)
            {
                string json = JsonSerializer.Serialize(table.Value);
                File.WriteAllText($"{dbPath}/{table.Key}.json", json);
            }
        }
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
                    Table table = JsonSerializer.Deserialize<Table>(json);
                    db.Tables[table.Name] = table;
                }

                dbms.Databases[dbName] = db;
            }
        }

        return dbms;
    }
}
