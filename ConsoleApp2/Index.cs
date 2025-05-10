using System.Runtime.Serialization;

[DataContract]
[KnownType(typeof(SerializableBTree))]
[KnownType(typeof(SerializableBTreeNode))]
public class Index
{
    [DataMember]
    public string ColumnName { get; set; }

    [IgnoreDataMember]
    private BTree btree;

    [DataMember]
    public SerializableBTree SerializableTree { get; set; }

    public Index(string columnName, int degree = 3)
    {
        ColumnName = columnName;
        btree = new BTree(degree);
    }

    public Index() { }

    private IComparable NormalizeKey(IComparable key)
    {
        return key is string s ? s.Trim().ToLowerInvariant() : key;
    }

    public void AddToIndex(IComparable key, object rowReference)
    {
        if (key is string sKey)
            key = sKey.Trim().ToLowerInvariant();

        var existing = btree.Search(key) as List<Dictionary<string, string>>;
        if (existing == null)
        {
            var newList = new List<Dictionary<string, string>> { (Dictionary<string, string>)rowReference };
            btree.Insert(key, newList);
            Console.WriteLine($"🆕 Inserted key '{key}' with 1 row");
        }
        else
        {
            existing.Add((Dictionary<string, string>)rowReference);
            Console.WriteLine($"➕ Appended to existing key '{key}', now has {existing.Count} rows");
        }
    }


    public void RemoveFromIndex(IComparable key, object rowReference)
    {
        key = NormalizeKey(key);

        var existing = btree.Search(key) as List<Dictionary<string, string>>;
        if (existing != null)
        {
            Console.WriteLine($"Before removal: {existing.Count} items");
            existing.RemoveAll(r => AreDictionariesEqual(r, (Dictionary<string, string>)rowReference));
            Console.WriteLine($"After removal: {existing.Count} items");

            if (existing.Count == 0)
            {
                Console.WriteLine("List is now empty. Proceeding to remove key.");
                btree.Remove(key);
            }
        }
    }



    public object Lookup(IComparable key)
    {
        key = NormalizeKey(key);
        return btree.Search(key);
    }

    public List<Dictionary<string, string>> LookupRows(IComparable key)
    {
        Console.WriteLine($"🔎 Searching for key: '{key}' in index '{ColumnName}'");
        key = NormalizeKey(key);
        var result = btree.Search(key);
        if (result is List<Dictionary<string, string>> list)
        {
            Console.WriteLine($"📦 Found list with {list.Count} items");
        }
        else if (result == null)
        {
            Console.WriteLine("📦 Result is null");
        }
        else
        {
            Console.WriteLine($"📦 Result is of type {result.GetType()} but not expected List<Dictionary<string, string>>");
        }

        return result as List<Dictionary<string, string>> ?? new List<Dictionary<string, string>>();
    }
    private bool AreDictionariesEqual(Dictionary<string, string> dict1, Dictionary<string, string> dict2)
    {
        if (dict1.Count != dict2.Count)
            return false;

        foreach (var kvp in dict1)
        {
            if (!dict2.TryGetValue(kvp.Key, out var value) || value != kvp.Value)
                return false;
        }

        return true;
    }

    public void PrepareForSave()
    {
        SerializableTree = btree.ToSerializable();
    }

    public void RestoreAfterLoad()
    {
        btree = BTree.FromSerializable(SerializableTree);
    }
    public void PrintKeys()
    {
        btree.PrintKeys();
    }
}
