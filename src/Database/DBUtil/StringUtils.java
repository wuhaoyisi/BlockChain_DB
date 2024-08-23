package Database.DBUtil;

import BTree.BTreeUtil.LeafNode;

public class StringUtils {
    public static String removeTrailingSpaces (String s){
        int endIndex = s.length() - 1;
        while (endIndex >= 0 && Character.isWhitespace(s.charAt(endIndex))) {
            endIndex--;
        }
        return s.substring(0, endIndex + 1);
    }
}
