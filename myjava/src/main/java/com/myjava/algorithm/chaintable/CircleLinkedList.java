package com.myjava.algorithm.chaintable;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/2 18:30
 * @Description:
 */
public class CircleLinkedList {
    public static void main(String[] args) {

    }
}

class Node3{
    private int id ;
    private Node3 next ;
    public Node3(int id) {
        this.id = id;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public Node3 getNext() {
        return next;
    }
    public void setNext(Node3 next) {
        this.next = next;
    }
    @Override
    public String toString() {
        return "Node3{" +
                "id=" + id +
                '}';
    }
}
