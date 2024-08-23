package BTree;

import BTree.BTreeUtil.BTreeLookupReturn;
import BTree.BTreeUtil.BTreeNode;
import BTree.BTreeUtil.InternalNode;
import BTree.BTreeUtil.LeafNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

// B-Tree data structure
public class BTree {

    // Root node of the B-Tree
    private BTreeNode root;

    /* t is the minimum degree of the B-Tree.
    * It represents the minimum number of children a node can have.
    * A valid B-Tree must satisfy the following properties:
    * t - 1 <= number of keys of non-root node <= 2 * t - 1
    * 1 <= number of keys of root node <= 2 * t - 1
    * t <= number of children of non-root node except leaves <= 2 * t
    * 2 <= number of children of root node except leaves <= 2 * t
    * number of children of leaf node = 0
    */

    // Minimum degree of the B-Tree
    private final int t = 6;
    // Maximum keys in a node
    private final int MAX_KEYS = 2 * t - 1;
    // Minimum keys in a node
    private final int MIN_KEYS = t - 1;
    private int nodeCount;


    private HashMap<Integer, Integer> keyBlock;

    // Constructor
    public BTree() {
        root = new LeafNode();
        nodeCount = 0;
        nodeCount++;
        keyBlock = new HashMap<>();
    }

    public BTreeNode getRoot() {
        return root;
    }

    // Splits a node into two nodes and redistributes keys and children
    // Parameters: parent - the parent node, index - the index of the node to be split
    private void split(InternalNode parent, int index) {
        // Prev node to be split
        BTreeNode prev = parent.getChildren().get(index);
        BTreeNode left;
        BTreeNode right;
        if (prev.isLeaf()) {
            // Creating new leaf nodes
            left = new LeafNode();
            right = new LeafNode();
        } else {
            // Creating new internal nodes
            left = new InternalNode();
            right = new InternalNode();
        }
        // Distributing keys to left and right nodes
        for (int i = 0; i < t - 1; i++) {
            left.getKeys().add(prev.getKeys().get(i));
            right.getKeys().add(prev.getKeys().get(i + t));
        }
        // Distributing children if not a leaf node
        if (!prev.isLeaf()) {
            for (int i = 0; i < t; i++) {
                assert left instanceof InternalNode;
                ((InternalNode) left).getChildren().add(((InternalNode) prev).getChildren().get(i));
                ((InternalNode) right).getChildren().add(((InternalNode) prev).getChildren().get(i + t));
            }
        }
        // Updating parent with the middle key
        parent.getKeys().add(index, prev.getKeys().get(t - 1));
        // Removing the original node and adding left and right nodes to the parent
        parent.getChildren().remove(index);
        parent.getChildren().add(index, left);
        parent.getChildren().add(index + 1, right);

        // Updating the node count

    }

    // Insert a value into the B-Tree and store the block number
    public void insert(int value, int block) {
        insert(value);
        keyBlock.put(value, block);
    }

    // Insert a value into the B-Tree
    public void insert(int value) {
        BTreeNode r = root;
        // If the root is full, create a new root and split it
        if (r.getKeys().size() == MAX_KEYS) {
            InternalNode s = new InternalNode();
            root = s;
            s.getChildren().add(r);
            split(s, 0);
            insertNonFull(s, value);
        } else {
            // Insert into non-full root
            insertNonFull(r, value);
        }
    }

    // Inserts a value into a non-full node of the B-Tree recursively
    // Parameters: node - the node to insert into, value - the value to be inserted
    private void insertNonFull(BTreeNode node, int value) {
        int i = node.getKeys().size() - 1;
        if (node.isLeaf()) {
            // Insert into leaf node
            while (i >= 0 && value < node.getKeys().get(i)) {
                i--;
            }
            node.getKeys().add(i + 1, value);
        } else {
            // Insert into internal node
            while (i >= 0 && value < node.getKeys().get(i)) {
                i--;
            }
            i++;
            List<BTreeNode> children = ((InternalNode) node).getChildren();
            if (children.get(i).getKeys().size() == MAX_KEYS) {
                split((InternalNode) node, i);
                if (value > node.getKeys().get(i)) {
                    i++;
                }
            }
            // Recursively insert into child
            insertNonFull(children.get(i), value);
        }
    }

    // Lookup a value
    public BTreeLookupReturn lookup(int value) {
        return lookup(root, value);
    }

    // Recursively looks up a value at a given node in the B-Tree
    // Parameters: node - the current node, value - the value to be looked up
    // Returns: BTreeLookupReturn object containing the node and index, or null if not found
    private BTreeLookupReturn lookup(BTreeNode node, int value) {
        int i = 0;
        List<Integer> keys = node.getKeys();
        // Search for the correct key in the node
        while (i < keys.size() && value > keys.get(i)) {
            i++;
        }
        // Found, return the node and index
        if (i < keys.size() && value == keys.get(i)) {
            return new BTreeLookupReturn(node, i);
        } else if (node.isLeaf()) {
            // If it's a leaf and key is not found, return null
            return null;
        } else {
            // Recursively search in child
            return lookup(((InternalNode) node).getChildren().get(i), value);
        }
    }


    public HashMap<Integer, Integer> getKeyBlock() {
        return keyBlock;
    }

    public void display() {
        display(root);
    }

    public void display(BTreeNode node) {
        display(node, 0);
    }

    // Displays the B-Tree recursively with indentation based on the level
    private void display(BTreeNode node, int level) {
        List<Integer> keys = node.getKeys();
        if (!node.isLeaf()) {
            List<BTreeNode> children = ((InternalNode) node).getChildren();
            for (int i = 0; i < children.size(); i++) {
                display(children.get(i), level + 1);
                if (i < keys.size()) {
                    System.out.println("    ".repeat(level) + keys.get(i));
                }
            }
        } else {
            for (Integer key : keys) {
                System.out.println("    ".repeat(level) + key);
            }
        }
    }

    public HashSet<BTreeNode> getNodes() {
        HashSet<BTreeNode> nodes = new HashSet<>();
        getNodes(root, nodes);
        return nodes;
    }

    private void getNodes(BTreeNode node, HashSet<BTreeNode> nodes) {
        nodes.add(node);
        if (!node.isLeaf()) {
            List<BTreeNode> children = ((InternalNode) node).getChildren();
            for (BTreeNode child : children) {
                getNodes(child, nodes);
            }
        }
    }

}