public class SerializableBTree:Index
{
    public SerializableBTreeNode Root { get; set; }
    public int Degree { get; set; }
}


public class SerializableBTreeNode
{
    public int[] Keys { get; set; }
    public object[] Values { get; set; }
    public List<SerializableBTreeNode> Children { get; set; } = new();
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

    public void Insert(int key, object value)
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

    public object Search(int key)
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
            Values = node.Values.Take(node.KeyCount).ToArray(),
            IsLeaf = node.IsLeaf
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
        sNode.Values.CopyTo(node.Values, 0);

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
