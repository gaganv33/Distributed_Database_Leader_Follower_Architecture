package merkleTree.node;

public class TreeNode {
    public final boolean isLeaf;
    public final String hash;
    public final String rightHash;
    public final String leftHash;
    public final TreeNode right;
    public final TreeNode left;

    public TreeNode(boolean isLeaf, String hash, String rightHash, String leftHash, TreeNode right, TreeNode left) {
        this.isLeaf = isLeaf;
        this.hash = hash;
        this.rightHash = rightHash;
        this.leftHash = leftHash;
        this.right = right;
        this.left = left;
    }
}
