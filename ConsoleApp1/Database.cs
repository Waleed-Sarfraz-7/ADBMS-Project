class Database
{
    public Dictionary<string, Table> Tables { get; set; }

    public Database()
    {
        Tables = new Dictionary<string, Table>();
    }

    public void CreateTable(string tableName, List<string> columns)
    {
        if (!Tables.ContainsKey(tableName))
        {
            Tables[tableName] = new Table(tableName, columns);
            Console.WriteLine($"Table '{tableName}' created successfully.");
        }
        else
        {
            Console.WriteLine("Table already exists.");
        }
    }
}
