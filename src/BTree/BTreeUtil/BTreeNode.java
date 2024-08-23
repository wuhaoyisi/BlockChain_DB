package BTree.BTreeUtil;// Abstract class representing B-tree node
import java.util.List;

abstract public class BTreeNode {

    // List of keys in the node
    List<Integer> keys;

    // Whether the node is a leaf node
    private boolean isLeaf;

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<Integer> getKeys() {
        return keys;
    }

    public void setKeys(List<Integer> keys) {
        this.keys = keys;
    }
}
