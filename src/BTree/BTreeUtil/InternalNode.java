package BTree.BTreeUtil;// Internal node in a B-Tree
import java.util.ArrayList;
import java.util.List;

public class InternalNode extends BTreeNode {

    // List of keys in the internal node
    private List<Integer> keys;

    // List of child nodes for the internal node
    private List<BTreeNode> children;

    private final boolean isLeaf = false;

    public InternalNode(List<Integer> keys, List<BTreeNode> children) {
        this.keys = keys;
        this.children = children;
    }

    public InternalNode() {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
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

    public List<BTreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<BTreeNode> children) {
        this.children = children;
    }

    public String toString() {
        return "InternalNode{" + "keys=" + keys;
    }

}
