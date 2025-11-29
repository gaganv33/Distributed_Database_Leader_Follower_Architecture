package merkleTree.construction;

import hash.Hash;
import merkleTree.node.LeafTreeNode;
import merkleTree.node.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TreeConstruction {
    /**
     * This method is responsible to construct the merkle tree using the given dataset.
     * @param data : We use the given dataset to construct the merkle tree.
     * @return : Returns the merkle tree for the given dataset.
     */
    public static TreeNode constructTreeNode(ConcurrentMap<String, String> data) {
        List<TreeNode> nodes = getLeafNodes(data);

        while (nodes.size() != 1) {
            int n = nodes.size();
            if ((n % 2 )!= 0) {
                nodes.add(nodes.get(n - 1));
                n++;
            }
            List<TreeNode> nextNodes = new ArrayList<>();

            for (int i = 0; i < n; i += 2) {
                TreeNode right = nodes.get(i);
                TreeNode left = nodes.get(i + 1);

                String rightHash = right.hash;
                String leftHash = left.hash;
                String hash = Hash.getHashForRightAndLeftSubtreeHash(rightHash, leftHash);

                nextNodes.add(new TreeNode(false, hash, rightHash, leftHash, right, left));
            }
            nodes = nextNodes;
        }

        return nodes.getFirst();
    }

    /**
     * This method is responsible to create leaf nodes for the given dataset.
     * Intermediate nodes : This node consists of hash of the right subtree and left subtree, right subtree hash value,
     *                      left subtree hash value, right node, left node.
     * Leaf Node : This node consists of hash of the key and the value, key and value.
     * @param data : It consists of the data, for which leaf node has to be created for each record.
     * @return : Returns the leaf node of the given merkle tree.
     */
    private static List<TreeNode> getLeafNodes(ConcurrentMap<String, String> data) {
        List<TreeNode> leafNodes = new ArrayList<>();

        for (var entry : data.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String hash = Hash.getHashForKeyAndValue(key, value);
            leafNodes.add(new LeafTreeNode(key, value, true, hash, null, null, null, null));
        }
        return leafNodes;
    }
}
