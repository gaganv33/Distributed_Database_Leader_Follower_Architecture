package merkleTree.node;

public class LeafTreeNode extends TreeNode {
    public final String key;
    public final String value;

    public LeafTreeNode(String key, String value, boolean isLeaf, String hash, String rightHash, String leftHash,
                        TreeNode right, TreeNode left) {
        super(isLeaf, hash, rightHash, leftHash, right, left);
        this.key = key;
        this.value = value;
    }
}
