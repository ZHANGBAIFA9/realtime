package com.myjava.algorithm.chaintable.chainTableOther;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/6/3 9:19
 * @Description:
 */
public class SingleLinked {
    public static void main(String[] args) {
        //创建节点
        Node node6 = new Node(6, "赵子龙",null);
        Node node5 = new Node(5, "曹操",node6);
        Node node4 = new Node(4, "项羽",node5);
        Node node3 = new Node(3, "张飞",node4);
        Node node2 = new Node(2, "关羽",node3);
        Node node1 = new Node(1, "刘备",node2);

//        iterate(node1);
        recursion(node1);
        System.out.println(node1);

    }

    public static Node iterate(Node head){
        Node prev = null , next ;
        Node curr = head ;
        while(curr != null){
            next = curr.next ;
            curr.next = prev ;
            prev = curr ;
            curr = next ;
        }
        return prev ;
    }
    public static Node recursion(Node head){
        if(head == null || head.next == null){
            return head ;
        }
        Node new_node = recursion(head.next);
        head.next.next = head ;
        head.next = null ;
        return new_node ;
    }
}
//节点类
class Node{
    public int id ;//序号
    public String name ;//姓名
    public Node next; //指向下一个节点的指针

    //给定构造
    public Node(int id, String name,Node next) {
        super() ;
        this.id = id;
        this.name = name;
        this.next = next ;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", next=" + next +
                '}';
    }
}