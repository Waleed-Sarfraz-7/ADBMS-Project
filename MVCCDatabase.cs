// Made this class just to only test the functionalities of the concurrenct control and this is just a demo and will make this
// User input is next task and to also optimize the query processor accordingly. 
// To not disturb the existing databases.

public class MVCCDatabase
{
    private Dictionary<string, TableforTest> tables = new();

    public void CreateTable(string tableName)
    {
        if (!tables.ContainsKey(tableName))
            tables[tableName] = new TableforTest();
    }

    public void Insert(Transaction tx, string tableName, Dictionary<string, string> row)
    {
        if (!tables.ContainsKey(tableName)) return;

        if (!tx.UncommittedChanges.ContainsKey(tableName))
            tx.UncommittedChanges[tableName] = new();

        tx.UncommittedChanges[tableName].Add(row);
    }

    public List<Dictionary<string, string>> Read(Transaction tx, string tableName)
    {
        if (!tables.ContainsKey(tableName)) return new();

        var committedRows = tables[tableName].Rows;
        var ownWrites = tx.UncommittedChanges.ContainsKey(tableName)
            ? tx.UncommittedChanges[tableName]
            : new List<Dictionary<string, string>>();

        return committedRows.Concat(ownWrites).ToList();
    }

    public void Commit(Transaction tx)
    {
        foreach (var (tableName, rows) in tx.UncommittedChanges)
        {
            if (!tables.ContainsKey(tableName)) continue;

            tables[tableName].Rows.AddRange(rows);
        }
    }

    public void Rollback(Transaction tx)
    {
        // Do nothing as uncommitted data is never applied to main tables
    }
}

public class TableforTest
{
    public List<Dictionary<string, string>> Rows = new();
}
