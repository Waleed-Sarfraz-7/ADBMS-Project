public class BTreeNode
{
    public int[] Keys;
    public object[] Values;
    public BTreeNode[] Children;
    public int Degree; // Minimum degree (defines the range for number of keys)
    public int KeyCount;
    public bool IsLeaf;

    public BTreeNode(int degree, bool isLeaf)
    {
        Degree = degree;
        IsLeaf = isLeaf;
        Keys = new int[2 * degree - 1];
        Values = new object[2 * degree - 1];
        Children = new BTreeNode[2 * degree];
        KeyCount = 0;
    }

    // A utility function to insert a new key in a non-full node
    public void InsertNonFull(int key, object value)
    {
        int i = KeyCount - 1;

        if (IsLeaf)
        {
            while (i >= 0 && Keys[i] > key)
            {
                Keys[i + 1] = Keys[i];
                Values[i + 1] = Values[i];
                i--;
            }

            Keys[i + 1] = key;
            Values[i + 1] = value;
            KeyCount++;
        }
        else
        {
            while (i >= 0 && Keys[i] > key)
                i--;

            if (Children[i + 1].KeyCount == 2 * Degree - 1)
            {
                SplitChild(i + 1, Children[i + 1]);

                if (Keys[i + 1] < key)
                    i++;
            }

            Children[i + 1].InsertNonFull(key, value);
        }
    }

    // Split the child y of this node at index i
    public void SplitChild(int i, BTreeNode y)
    {
        var z = new BTreeNode(y.Degree, y.IsLeaf);
        z.KeyCount = Degree - 1;

        for (int j = 0; j < Degree - 1; j++)
        {
            z.Keys[j] = y.Keys[j + Degree];
            z.Values[j] = y.Values[j + Degree];
        }

        if (!y.IsLeaf)
        {
            for (int j = 0; j < Degree; j++)
                z.Children[j] = y.Children[j + Degree];
        }

        for (int j = KeyCount; j >= i + 1; j--)
            Children[j + 1] = Children[j];

        Children[i + 1] = z;

        for (int j = KeyCount - 1; j >= i; j--)
        {
            Keys[j + 1] = Keys[j];
            Values[j + 1] = Values[j];
        }

        Keys[i] = y.Keys[Degree - 1];
        Values[i] = y.Values[Degree - 1];

        KeyCount++;
        y.KeyCount = Degree - 1;
    }

    public object Search(int key)
    {
        int i = 0;
        while (i < KeyCount && key > Keys[i])
            i++;

        if (i < KeyCount && Keys[i] == key)
            return Values[i];

        return IsLeaf ? null : Children[i].Search(key);
    }
}
