using System.Text.Json.Serialization;

public class Index
{
    public string ColumnName { get; }
    [JsonIgnore]
    private BTree btree;
    public SerializableBTree SerializableTree { get; set; }


    public Index(string columnName, int degree = 3)
    {
        ColumnName = columnName;
        btree = new BTree(degree);
    }
    public Index() { }

    public void AddToIndex(int key, object rowReference)
    {
        btree.Insert(key, rowReference);
    }

    public object Lookup(int key)
    {
        return btree.Search(key);
    }
    public void PrepareForSave()
    {
        SerializableTree = btree.ToSerializable();
    }

    public void RestoreAfterLoad()
    {
        btree = BTree.FromSerializable(SerializableTree);
    }
}
