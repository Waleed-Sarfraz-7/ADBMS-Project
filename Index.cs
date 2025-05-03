using System.Runtime.Serialization;
using System.Text.Json.Serialization;
[DataContract]
[KnownType(typeof(SerializableBTree))]

public class Index
{
    
    public string ColumnName { get; }
    
    private BTree btree;
    
    public SerializableBTree SerializableTree { get; set; }


    public Index(string columnName, int degree = 3)
    {
        ColumnName = columnName;
        btree = new BTree(degree);
    }
    public Index() { }

    public void AddToIndex(string key, object rowReference)
    {
        // Assuming you modify BTree to accept string keys too
        btree.Insert(key, rowReference);
    }

    public void AddToIndex(int key, object rowReference)
    {
        // Assuming you modify BTree to accept string keys too
        btree.Insert(key, rowReference);
    }
    public object Lookup(int key)
    {
        return btree.Search(key);
    }

    public object Lookup(string key)
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
