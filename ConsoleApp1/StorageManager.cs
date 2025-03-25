using System.IO;
using System.Text.Json;

class StorageManager
{
    private const string DataPath = "data";

    public static void SaveDatabase(Database db)
    {
        Directory.CreateDirectory(DataPath);
        foreach (var table in db.Tables)
        {
            string json = JsonSerializer.Serialize(table.Value);
            File.WriteAllText($"{DataPath}/{table.Key}.json", json);
        }
    }

    public static Database LoadDatabase()
    {
        Database db = new Database();
        if (Directory.Exists(DataPath))
        {
            foreach (var file in Directory.GetFiles(DataPath, "*.json"))
            {
                string json = File.ReadAllText(file);
                Table table = JsonSerializer.Deserialize<Table>(json);
                db.Tables[table.Name] = table;
            }
        }
        return db;
    }
}
