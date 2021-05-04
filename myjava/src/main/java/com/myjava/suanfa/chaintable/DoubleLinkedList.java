package com.myjava.suanfa.chaintable;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/2 14:23
 * @Description:
 */
public class DoubleLinkedList {
    public static void main(String[] args) {
        DoubleLinkedListDemo doubleLinkedListDemo = new DoubleLinkedListDemo();
        //创建节点
        Node2 node1 = new Node2(1, "刘备");
        Node2 node2 = new Node2(2, "关羽");
        Node2 node3 = new Node2(3, "张飞");
        Node2 node4 = new Node2(4, "项羽");
        Node2 node5 = new Node2(5, "曹操");
        Node2 node6 = new Node2(6, "赵子龙");
//        doubleLinkedListDemo.addNode2(node1);
//        doubleLinkedListDemo.addNode2(node2);
//        doubleLinkedListDemo.addNode2(node3);
//        doubleLinkedListDemo.addNode2(node4);
//        doubleLinkedListDemo.addNode2(node5);
//        doubleLinkedListDemo.addNode2(node6);
        //无序添加
        doubleLinkedListDemo.addReOrderNode2(node6);
        doubleLinkedListDemo.addReOrderNode2(node2);
        doubleLinkedListDemo.addReOrderNode2(node5);
        doubleLinkedListDemo.addReOrderNode2(node4);
        doubleLinkedListDemo.addReOrderNode2(node3);
        doubleLinkedListDemo.addReOrderNode2(node1);
        //显示链表
        doubleLinkedListDemo.showNode2(doubleLinkedListDemo.getNode2());
        //修改链表中第4个节点数据为 小乔
//        System.out.println("------------------修改节点后链表-------------------");
//        doubleLinkedListDemo.updateNode2(new Node2(4,"小乔"));
//        doubleLinkedListDemo.showNode2(doubleLinkedListDemo.getNode2());
        //测试删除节点
        System.out.println("------------------删除节点后链表-------------------");
        doubleLinkedListDemo.delNode2(6);
        doubleLinkedListDemo.showNode2(doubleLinkedListDemo.getNode2());

    }
}
class DoubleLinkedListDemo{
    //初始化头节点
    private Node2 head = new Node2(0 ,"") ;
    //通过该方法获取头节点
    public Node2 getNode2(){
        return head ;
    }
    //添加节点，单链表需要一直遍历到最后节点进行添加
    public void addNode2(Node2 node){
        Node2 tmp = head ;
        while(true){
            if(null == tmp.next ){
                break ;
            }
            tmp = tmp.next ;
        }
        tmp.next = node;
        node.prev = tmp ;
    }
    //有序添加节点
    public void addReOrderNode2(Node2 node){
        //创建头节点
        Node2 tmp = head;
        //创建辅助节点
        Node2 current = new Node2(0,"");
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
            if(null != tmp.next){
                //直接搞
//                node.next = tmp.next;
//                tmp.next = node;
//                node.prev = tmp ;
//                node.next.prev = node ;
                //利用中间变量进行辅助
                current.next = tmp.next ;
                tmp.next = node ;
                node.prev = tmp ;
                node.next = current.next ;
                current.next.prev = node ;
            }else{
                tmp.next = node ;
                node.prev = tmp ;
            }
        }else{
            System.out.println("id: "+node.id + "重复");
        }
    }
    // 显示链表
    public void showNode2(Node2 node){
        if( null ==node.next){
            System.out.println("链表为空");
            return;
        }
        //头节点固定
        Node2 tmp = node.next ;
        while(true){
            System.out.println(tmp);
            if( null == tmp.next){
                break ;
            }
            tmp = tmp.next ;
        }
    }
    //修改链表节点数据
    public void updateNode2(Node2 node){
        if(null == head.next){
            System.out.println("链表为空");
            return;
        }
        Node2 tmp = head.next ;
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
    public void delNode2(int id){
        Node2 tmp = head.next ;
        boolean flag = false ;
        while(true){
            if(id == tmp.id){
                flag = true ;
                break ;
            }
            if(null == tmp.next){
                break ;
            }
            tmp = tmp.next ;
        }
        //后面节点前移
        if(flag){
//            tmp.next = tmp.next.next ;
            tmp.prev.next = tmp.next ;
            if(null != tmp.next){
                tmp.next.prev = tmp.prev ;
            }
        }else{
            System.out.println("没有找到id为："+id+"的节点");
        }
    }
}

class Node2{
    public int id ;//序号
    public String name ;//姓名
    public Node2 next; //指向下一个节点的指针
    public Node2 prev; //指向前一个节点
    //给定构造
    public Node2(int id, String name) {
        super() ;
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Node2{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
