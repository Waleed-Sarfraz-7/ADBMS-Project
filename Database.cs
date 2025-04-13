using ConsoleApp1;
class Database
{
    public Dictionary<string, Table> Tables { get; set; }
    public string Name { get; set; }

    public Database(string name)
    {
        Name = name;
        Tables = new Dictionary<string, Table>();
    }
    public Database()
    {
        Tables = new Dictionary<string, Table>();
    }

    public void CreateTable(string tableName, List<Column> columns)
    {
        if (!Tables.ContainsKey(tableName))
        {
            var table = new Table(tableName, columns, this); // Pass current Database as parent
            Tables[tableName] = table;
            Console.WriteLine($"Table '{tableName}' created successfully.");
        }
        else
        {
            Console.WriteLine("Table already exists.");
        }
    }


}
