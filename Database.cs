using ConsoleApp1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
[DataContract]
class Database
{
    [DataMember]
    public Dictionary<string, Table> Tables { get; set; }
    [DataMember]
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

    // Create a table and also create indexes for PrimaryKey and Unique constraints
    public void CreateTable(string tableName, List<Column> columns)
    {
        if (!Tables.ContainsKey(tableName))
        {
            var table = new Table(tableName, columns, this); // Pass current Database as parent

            // Create indexes for PrimaryKey and Unique constraints
            foreach (var column in columns)
            {
                if (column.Constraint.Has(ConstraintType.PrimaryKey) || column.Constraint.Has(ConstraintType.Unique))
                {
                    table.CreateIndex(column.Name);  // Create an index on the column
                }
            }

            Tables[tableName] = table;
            Console.WriteLine($"Table '{tableName}' created successfully.");
        }
        else
        {
            Console.WriteLine("Table already exists.");
        }
    }

    // Insert a row into the table and handle transaction logging
    public void InsertRow(Guid transactionId, string tableName, Dictionary<string, string> row, TransactionManager tm)
    {
        if (!Tables.ContainsKey(tableName))
        {
            Console.WriteLine($"Table {tableName} does not exist.");
            return;
        }

        var table = Tables[tableName];

        // Check constraints (basic)
        foreach (var col in table.Columns)
        {
            if (!row.ContainsKey(col.Name) || string.IsNullOrEmpty(row[col.Name]))
            {
                if (col.Constraint.Has(ConstraintType.NotNull))
                {
                    Console.WriteLine($"Constraint violation: Column '{col.Name}' cannot be null.");
                    return;
                }
            }
        }

        // Lock row for insert (this is where the transaction logging happens)
        tm.LogOperation(transactionId, "insert", tableName, null, new List<Dictionary<string, string>> { row }, this);

        // Add row to the table
        table.Rows.Add(row);

        // Update indexes after insertion
        foreach (var col in table.Columns)
        {
            if (table.Indexes.ContainsKey(col.Name))
            {
                var index = table.Indexes[col.Name];
                index.AddToIndex(row[col.Name], row);
            }
        }

        Console.WriteLine($"Row inserted into table '{tableName}' successfully.");
    }

    // Get the table object by its name
    public Table GetTable(string tableName)
    {
        return Tables.ContainsKey(tableName) ? Tables[tableName] : null;
    }
}
