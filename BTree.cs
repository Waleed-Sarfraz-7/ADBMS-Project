using System.Runtime.Serialization;
using System.Text.Json.Serialization;
[DataContract]
public class SerializableBTree : Index
{
    [DataMember]
    public SerializableBTreeNode Root { get; set; }
    [DataMember]
    public int Degree { get; set; }
}
[DataContract]
public class SerializableBTreeNode
{
    [DataMember]
    public object[] Keys { get; set; }
    [DataMember]
    [JsonIgnore]
    public object[] Values { get; set; }
    [DataMember]
    public int[] ValueReferences { get; set; }
    [DataMember]
    public List<SerializableBTreeNode> Children { get; set; } = new();
    [DataMember]
    public bool IsLeaf { get; set; }
}
[DataContract]
public class BTree
{
    private BTreeNode root;
    private int degree;

    public BTree(int degree)
    {
        this.degree = degree;
        root = new BTreeNode(degree, true);
    }

    public void Insert(IComparable key, object value)
    {
        if (root.KeyCount == 2 * degree - 1)
        {
            var newRoot = new BTreeNode(degree, false);
            newRoot.Children[0] = root;
            newRoot.SplitChild(0, root);
            root = newRoot;
        }

        root.InsertNonFull(key, value);
    }

    public object Search(IComparable key)
    {
        return root.Search(key);
    }

    public SerializableBTree ToSerializable()
    {
        return new SerializableBTree
        {
            Degree = degree,
            Root = ConvertNode(root)
        };
    }

    private SerializableBTreeNode ConvertNode(BTreeNode node)
    {
        var sNode = new SerializableBTreeNode
        {
            Keys = node.Keys.Take(node.KeyCount).ToArray(),
            Values = node.Values.Take(node.KeyCount).ToArray(),   // Keep for runtime
            IsLeaf = node.IsLeaf,
            ValueReferences = Enumerable.Range(0, node.KeyCount).ToArray()   // Save dummy simple references (or your actual logic)
        };

        if (!node.IsLeaf)
        {
            for (int i = 0; i <= node.KeyCount; i++)
            {
                sNode.Children.Add(ConvertNode(node.Children[i]));
            }
        }

        return sNode;
    }


    public static BTree FromSerializable(SerializableBTree sTree)
    {
        var tree = new BTree(sTree.Degree);
        tree.root = ConvertNodeBack(sTree.Root, sTree.Degree);
        return tree;
    }

    private static BTreeNode ConvertNodeBack(SerializableBTreeNode sNode, int degree)
    {
        var node = new BTreeNode(degree, sNode.IsLeaf)
        {
            KeyCount = sNode.Keys.Length
        };

        sNode.Keys.CopyTo(node.Keys, 0);

        // Values will be null initially, you can recover later if needed
        node.Values = new object[2 * degree - 1];

        if (!sNode.IsLeaf)
        {
            for (int i = 0; i < sNode.Children.Count; i++)
            {
                node.Children[i] = ConvertNodeBack(sNode.Children[i], degree);
            }
        }

        return node;
    }

}
