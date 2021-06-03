package com.myjava.algorithm.chaintable;

import java.util.Stack;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/31 17:36
 * @Description:
 */
public class SingleLinkedList {
    public static void main(String[] args) {
        SingleLinkedListDemo singleLinkedListDemo = new SingleLinkedListDemo();
        //创建节点
        Node node1 = new Node(1, "刘备");
        Node node2 = new Node(2, "关羽");
        Node node3 = new Node(3, "张飞");
        Node node4 = new Node(4, "项羽");
        Node node5 = new Node(5, "曹操");
        Node node6 = new Node(6, "赵子龙");
        //添加节点到链表,有序添加
//        singleLinkedListDemo.addNode(node1);
//        singleLinkedListDemo.addNode(node2);
//        singleLinkedListDemo.addNode(node3);
//        singleLinkedListDemo.addNode(node4);
//        singleLinkedListDemo.addNode(node5);
//        singleLinkedListDemo.addNode(node6);
        //无序添加
        singleLinkedListDemo.addReOrderNode(node5);
        singleLinkedListDemo.addReOrderNode(node3);
        singleLinkedListDemo.addReOrderNode(node1);
        singleLinkedListDemo.addReOrderNode(node6);
        singleLinkedListDemo.addReOrderNode(node4);
        singleLinkedListDemo.addReOrderNode(node2);
        //显示链表
        singleLinkedListDemo.showNode(singleLinkedListDemo.getNode());
        //修改链表节点项羽为貂蝉
//        System.out.println("*************************************");
//        singleLinkedListDemo.updateNode(new Node(4,"貂蝉"));
//        singleLinkedListDemo.showNode(singleLinkedListDemo.getNode());
        //删除节点
    //        System.out.println("*************************************");
    //        singleLinkedListDemo.delNode(4);
    //        singleLinkedListDemo.showNode(singleLinkedListDemo.getNode());
        //打印链表长度
        System.out.println(getLinkedLength(singleLinkedListDemo.getNode()));
        //获取节点后三位
//        System.out.println(getLastNode(singleLinkedListDemo.getNode(),3));
        //反转链表
//        System.out.println("---------------------------");
//        linkedReverse(singleLinkedListDemo.getNode());
//        singleLinkedListDemo.showNode(singleLinkedListDemo.getNode());
        //压栈弹栈实现链表逆序
//        linkedReverseStack(singleLinkedListDemo.getNode());
        //合并链表
        SingleLinkedListDemo singleLinkedListDemo2 = new SingleLinkedListDemo();
        Node node7 = new Node(7,"貂蝉");
        Node node8 = new Node(8,"王昭君");
        Node node9 = new Node(9,"诸葛亮");
        singleLinkedListDemo2.addReOrderNode(node7);
        singleLinkedListDemo2.addReOrderNode(node8);
        singleLinkedListDemo2.addReOrderNode(node9);
        mergeLinked(singleLinkedListDemo2.getNode(),singleLinkedListDemo.getNode());
    }

    //遍历链表长度
    public static int getLinkedLength(Node node){
        //判断链表是否为空
        if(null == node.next){
            return 0;
        }
        Node tmp = node.next ;
        int len = 0 ;
        while(tmp != null){
            len++;
            tmp = tmp.next ;
        }
        return len ;
    }
    //查找链表倒数第三位节点,头节点传进来
    public static Node getLastNode(Node node,int num){
        if(null == node.next){
            return null ;
        }
        //获取链表长度,node 代表头节点
        int len = getLinkedLength(node);
        //首先判断num是否越界
        if(num <= 0 || num > len){
            return null;
        }
        //临时节点辅助
        Node tmp = node.next;
        for(int i = 0 ; i< len - num; i++){
            tmp = tmp.next;
        }
        return tmp ;
    }
    //反转链表
    public static void linkedReverse(Node node){
        //判断链表是否为空，或者链表长度是否为1，如为1不需反转
        if(null == node.next || null == node.next.next){
            return;
        }
        //定义辅助指针变量
        Node tmp = node.next ;
        Node next = null ;
        //新建指向当前节点的reverseHead
        Node reverseHead = new Node(0,"");
        //遍历节点，将遍历的节点存入到reverseHead中
        while(tmp != null){
            //暂时保存当前节点的下一个节点
            next = tmp.next;
            // 将数据指向reverseHead这个链表的前端
            tmp.next = reverseHead.next ;
            //指向当前节点，不然会是null
            reverseHead.next = tmp ;
            //后移
            tmp = next ;
        }
        //reverseHead这个链表里就是反转后的链表，所以head指向reverseHead就得到一个反转的链表
        node.next = reverseHead.next ;
    }

    //利用栈特性（先进后出），来实现逆序
    public static void linkedReverseStack(Node node){
        if(null == node.next){
            return;
        }
        Stack<Node> stack = new Stack<Node>();
        Node tmp = node.next ;
        //压栈
        while(tmp != null){
            stack.push(tmp);
            tmp = tmp.next;
        }
        //弹栈
        while(stack.size() > 0){
            System.out.println(stack.pop());
        }
    }
    // 链表合并
    public static void mergeLinked(Node node1,Node node2){
        Node tmp1 = node1.next ;
        Node tmp2 = node2.next ;
        //创建中间临时链表用来合并链表
        Node node3 = new Node(0,"");
        Node tmp3 = node3 ;
        //循环合并, 1、tmp1 有数据 tmp2 无数据 2、tmp1 无数据 tmp2 有数据 3、tmp1和tmp2都没有数据
        while(null != tmp1 || null != tmp2){
            if(null != tmp1 && null ==tmp2){
                tmp3.next = tmp1;
                tmp1 = tmp1.next ;
            }else if(null != tmp2 && null == tmp1){
                tmp3.next = tmp2 ;
                tmp2 = tmp2.next ;
            }else{
                if(tmp1.id < tmp2.id){
                    tmp3.next = tmp1;
                    tmp1 = tmp1.next ;
                }else{
                    tmp3.next = tmp2 ;
                    tmp2 = tmp2.next ;
                }
            }
            tmp3 = tmp3.next ;
        }
        SingleLinkedListDemo singleLinkedListDemo = new SingleLinkedListDemo();
        singleLinkedListDemo.showNode(node3);
    }
}

class SingleLinkedListDemo{
    //初始化头节点
    private Node head = new Node(0 ,"") ;
    //通过该方法获取头节点
    public Node getNode(){
        return head ;
    }
    //添加节点，单链表需要一直遍历到最后节点进行添加
    public void addNode(Node node){
        Node tmp = head ;
        while(true){
            if(null == tmp.next ){
                break ;
            }
            tmp = tmp.next ;
        }
        tmp.next = node;
    }
    //有序添加节点
    public void addReOrderNode(Node node){
        Node tmp = head;
        boolean flag = false ;
        while(true){
            if(null == tmp.next){
                flag = true ;
                break;
            }else if(tmp.next.id > node.id){
                flag = true ;
                break ;
            }else if(tmp.next.id == node.id){
                break;
            }
            tmp = tmp.next ;
        }
        if(flag){
            //tmp 序号id大于新增节点，新增节点指向tmp节点
            node.next = tmp.next;
            //tmp 节点
            tmp.next = node ;
        }else{
            System.out.println("id: "+node.id + "重复");
        }
    }
    // 显示链表
    public void showNode(Node node){
        if( null ==node.next){
            System.out.println("链表为空");
            return;
        }
        //头节点固定
        Node tmp = node.next ;
        while(true){
            System.out.println(tmp);
            if( null == tmp.next){
                break ;
            }
            tmp = tmp.next ;
        }
    }
    //修改链表节点数据
    public void updateNode(Node node){
        if(null == head.next){
            System.out.println("链表为空");
            return;
        }
        Node tmp = head.next ;
        boolean flag = false ;
        while(true){
            //找到
            if(tmp.id == node.id){
                flag = true ;
                break ;
            }
            //找不到，防止空指针异常
            if(null == tmp.next){
                break;
            }
            tmp = tmp.next ;
        }
        if(flag){
            tmp.name = node.name ;
        }else{
            System.out.println("没有找到id为："+node.id);
        }
    }
    // 删除节点
    public void delNode(int id){
        Node tmp = head ;
        boolean flag = false ;
        while(true){
            if(null == tmp.next){
                break ;
            }
            if(id == tmp.next.id){
                flag = true ;
                break ;
            }
            tmp = tmp.next ;
        }
        //后面节点前移
        if(flag){
            tmp.next = tmp.next.next ;
        }else{
            System.out.println("没有找到id为："+id+"的节点");
        }
    }
}
//节点类
class Node{
    public int id ;//序号
    public String name ;//姓名
    public Node next; //指向下一个节点的指针

    //给定构造
    public Node(int id, String name) {
        super() ;
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
