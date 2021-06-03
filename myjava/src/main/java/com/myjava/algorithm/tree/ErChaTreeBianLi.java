package com.myjava.algorithm.tree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/31 17:20
 * @Description:
 */
public class ErChaTreeBianLi {
    public static void main(String[] args) {
        TreeNode treeNode = new TreeNode();

        System.out.println(PrintFromTopToBottom(treeNode));
    }

    public static ArrayList<Integer> PrintFromTopToBottom(TreeNode root) {
        ArrayList<Integer> resultList = new ArrayList<>();
        if (root == null) {
            return resultList;
        }
        Queue<TreeNode> q = new LinkedList<>();
        q.add(root);
        while (!q.isEmpty()) {
            TreeNode nowNode = q.peek();
            q.poll();
            resultList.add(nowNode.val);
            if (nowNode.left != null) {
                q.add(nowNode.left);
            }
            if (nowNode.right != null) {
                q.add(nowNode.right);
            }
        }
        return resultList;
    }
}
class TreeNode {
    int val;
    TreeNode left, right;
}

