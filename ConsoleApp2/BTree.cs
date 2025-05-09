using System.Runtime.Serialization;

[DataContract]
public class SerializableBTree
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
    public int[] ValueReferences { get; set; } // Optional for external linking

    [DataMember]
    public List<SerializableBTreeNode> Children { get; set; } = new();

    [DataMember]
    public bool IsLeaf { get; set; }
}

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

    public void Remove(IComparable key, object value)
    {
        root?.Remove(key, value);
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

    public static BTree FromSerializable(SerializableBTree sTree)
    {
        var tree = new BTree(sTree.Degree);
        tree.root = ConvertNodeBack(sTree.Root, sTree.Degree);
        return tree;
    }

    private static SerializableBTreeNode ConvertNode(BTreeNode node)
    {
        var sNode = new SerializableBTreeNode
        {
            Keys = node.Keys.Take(node.KeyCount).ToArray(),
            IsLeaf = node.IsLeaf,
            ValueReferences = Enumerable.Range(0, node.KeyCount).ToArray()
        };

        if (!node.IsLeaf)
        {
            for (int i = 0; i <= node.KeyCount; i++)
                sNode.Children.Add(ConvertNode(node.Children[i]));
        }

        return sNode;
    }
    public void PrintKeys()
    {
        root.PrintKeys();
    }
    private static BTreeNode ConvertNodeBack(SerializableBTreeNode sNode, int degree)
    {
        var node = new BTreeNode(degree, sNode.IsLeaf)
        {
            KeyCount = sNode.Keys.Length,
            Keys = new IComparable[2 * degree - 1]
        };

        for (int i = 0; i < sNode.Keys.Length; i++)
            node.Keys[i] = (IComparable)sNode.Keys[i];

        node.Values = new object[2 * degree - 1];
        node.Children = new BTreeNode[2 * degree];

        if (!sNode.IsLeaf)
        {
            for (int i = 0; i < sNode.Children.Count; i++)
                node.Children[i] = ConvertNodeBack(sNode.Children[i], degree);
        }

        return node;
    }
}
