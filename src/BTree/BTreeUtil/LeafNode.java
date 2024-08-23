package BTree.BTreeUtil;// Leaf node in a B-Tree
import java.util.ArrayList;
import java.util.List;

public class LeafNode extends BTreeNode{

    // List of keys in the leaf node
    private List<Integer> keys;

    private final boolean isLeaf = true;

    public LeafNode(List<Integer> keys) {
        this.keys = keys;
    }

    public LeafNode() {
        this.keys = new ArrayList<Integer>();
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<Integer> getKeys() {
        return keys;
    }

    public void setKeys(List<Integer> keys) {
        this.keys = keys;
    }

    //Convert the leaf node to an internal node
    public InternalNode toInternalNode() {
        return new InternalNode(this.keys, new ArrayList<BTreeNode>());
    }

    public String toString() {
        return "LeafNode{" + "keys=" + keys + '}';
    }
}
