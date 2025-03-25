class Table
{
    public string Name { get; set; }
    public List<string> Columns { get; set; }
    public List<Dictionary<string, string>> Rows { get; set; }

    public Table(string name, List<string> columns)
    {
        Name = name;
        Columns = columns;
        Rows = new List<Dictionary<string, string>>();
    }

    public void InsertRow(Dictionary<string, string> row)
    {
        Rows.Add(row);
    }

    public void DisplayTable()
    {
        Console.WriteLine($"Table: {Name}");
        Console.WriteLine(string.Join(" | ", Columns));
        foreach (var row in Rows)
        {
            Console.WriteLine(string.Join(" | ", Columns.Select(c => row.ContainsKey(c) ? row[c] : "NULL")));
        }
    }
}
