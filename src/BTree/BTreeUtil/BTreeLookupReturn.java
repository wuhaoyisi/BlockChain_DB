package BTree.BTreeUtil;

// Result of a B-Tree lookup operation
public class BTreeLookupReturn {

    // The node where the lookup result was found
    private BTreeNode node;

    // The index within the node where the lookup result was found
    private int index;

    public BTreeLookupReturn(BTreeNode node, int index) {
        this.node = node;
        this.index = index;
    }

    public BTreeNode getNode() {
        return node;
    }

    public int getIndex() {
        return index;
    }

    public void setNode(BTreeNode node) {
        this.node = node;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
