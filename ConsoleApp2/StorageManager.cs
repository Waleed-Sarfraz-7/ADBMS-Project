using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections.Generic;
using ConsoleApp1;

[Serializable]
class BinaryStorageManager
{
    private const string RootDataPath = "F:\\Semester 4\\DB Project\\C#\\bin\\Debug\\net8.0\\binary_data";

    public static void SaveDBMS(DBMS dbms)
    {
        Directory.CreateDirectory(RootDataPath);
        foreach (var dbEntry in dbms.Databases)
        {
            string dbPath = Path.Combine(RootDataPath, dbEntry.Key);
            Directory.CreateDirectory(dbPath);

            foreach (var table in dbEntry.Value.Tables)
            {
                string tablePath = Path.Combine(dbPath, table.Key);
                Directory.CreateDirectory(tablePath);

                // Save table with filtered constraints
                Table cleanedTable = CleanTable(table.Value);
                string tableFile = Path.Combine(tablePath, $"{table.Key}.bin");
                SaveBinary(cleanedTable, tableFile);

                // Save indexes
                if (table.Value.Indexes != null && table.Value.Indexes.Count > 0)
                {
                    string indexPath = Path.Combine(tablePath, "indexes");
                    Directory.CreateDirectory(indexPath);

                    foreach (var index in table.Value.Indexes)
                    {
                        index.Value.PrepareForSave(); // Correct: this is an Index object
                        string indexFile = Path.Combine(indexPath, $"{index.Key}.bin");
                        SaveBinary(index.Value, indexFile);
                    }


                }


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

                foreach (var tableFolder in Directory.GetDirectories(dbFolder))
                {
                    string tableName = Path.GetFileName(tableFolder);
                    string tableFile = Path.Combine(tableFolder, $"{tableName}.bin");

                    if (File.Exists(tableFile))
                    {
                        Table table = LoadBinary<Table>(tableFile);
                        table.SetParentDatabase(db);

                        foreach (var column in table.Columns)
                        {
                            column.SetParentTable(table);
                        }

                        LoadIndexesForTable(table, tableFolder);
                        table.Rows = table.Rows
    .Select(row => DeserializeRow(row, table.Columns)
                    .ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? ""))
    .ToList();

                        db.Tables[table.Name] = table;
                        Console.WriteLine("table loaded Successfully", table.Name);

                    }
                }

                dbms.Databases[dbName] = db;

            }
        }

        Console.WriteLine("DBMS Loaded Suceesfully");
        return dbms;
    }

    private static void LoadIndexesForTable(Table table, string tableFolder)
    {
        string indexesFolder = Path.Combine(tableFolder, "indexes");

        if (Directory.Exists(indexesFolder))
        {
            foreach (var indexFile in Directory.GetFiles(indexesFolder, "*.bin"))
            {
                string indexName = Path.GetFileNameWithoutExtension(indexFile);
                Index index = LoadBinary<Index>(indexFile);
                index.RestoreAfterLoad(); // <-- Properly restores BTree from SerializableTree
                table.Indexes[indexName] = index;

                //index.PrintKeys();
                Console.WriteLine($"Loaded index '{indexName}' for table '{table.Name}'.");
            }
        }
    }

    private static readonly List<Type> KnownTypes = new()
{
    typeof(SerializableBTree),
    typeof(SerializableBTreeNode),
    typeof(List<Dictionary<string, string>>),
    typeof(Dictionary<string, string>)
};

    private static void SaveBinary<T>(T obj, string path)
    {
        using (FileStream stream = new FileStream(path, FileMode.Create))
        {
            var serializer = new DataContractSerializer(typeof(T), KnownTypes);
            serializer.WriteObject(stream, obj);
        }
    }

    private static T LoadBinary<T>(string path)
    {
        using (FileStream stream = new FileStream(path, FileMode.Open))
        {
            var serializer = new DataContractSerializer(typeof(T), KnownTypes);
            return (T)serializer.ReadObject(stream);
        }
    }



    private static Table CleanTable(Table table)
    {
        var newTable = new Table(table.Name, new List<Column>(), table.ParentDatabase)
        {
            Rows = table.Rows.Select(row =>
            {
                var converted = new Dictionary<string, string>();
                foreach (var kv in row)
                {
                    converted[kv.Key] = kv.Value?.ToString();
                }
                return converted;
            }).ToList(),

            Indexes = table.Indexes
        };

        foreach (var col in table.Columns)
        {
            var cleanConstraints = new ColumnConstraint
            {
                Constraints = col.Constraint.Constraints & ~ConstraintType.None,
                DefaultValue = col.Constraint.DefaultValue,
                CheckExpression = col.Constraint.CheckExpression,
                ReferenceTable = col.Constraint.ReferenceTable,
                ReferenceColumn = col.Constraint.ReferenceColumn
            };

            var newCol = new Column(col.Name, col.Data_Type, null, cleanConstraints);
            newCol.SetParentTable(newTable);
            newTable.Columns.Add(newCol);
        }

        return newTable;
    }

    private static Dictionary<string, object> DeserializeRow(Dictionary<string, string> rawRow, List<Column> columns)
    {
        var deserialized = new Dictionary<string, object>();

        foreach (var column in columns)
        {
            string rawValue = rawRow[column.Name];
            object typedValue = rawValue;

            if (column.Data_Type == "bool")
            {
                typedValue = bool.Parse(rawValue);
            }
            else if (column.Data_Type == "int")
            {
                typedValue = int.Parse(rawValue);
            }
            else if (column.Data_Type == "float")
            {
                typedValue = float.Parse(rawValue);
            }
            // Add more types as needed

            deserialized[column.Name] = typedValue;
        }

        return deserialized;
    }

}