package com.myjava.threads.produceAndConsumer;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 15:01
 * @Description:    线程通信之生产消费问题，存在共享数据问题
 */
class Clert{

    //产品
    private int productCount = 0 ;

    //生产产品
    public synchronized void produceProduct() {
        if(productCount < 20){
            productCount++ ;
            System.out.println(Thread.currentThread().getName()+":开始生产第" + productCount + "个产品");
            notify();
        }else{
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    //消费产品
    public synchronized void consumProduct() {
        if(productCount > 0 ){
            System.out.println(Thread.currentThread().getName() + ":消费者开始消费第" + productCount + "个产品");
            productCount -- ;
            notify();
        }else{
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
class Producer extends Thread{
    private Clert clert ;

    public Producer(Clert clert) {
        this.clert = clert;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "生产者开始生产...");
        while (true){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            clert.produceProduct();
        }
    }
}
class Consumer extends Thread{
    private Clert clert ;

    public Consumer(Clert clert) {
        this.clert = clert;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "消费者开始消费...");
        while (true){
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            clert.consumProduct();
        }
    }
}
public class ProduceTest {
    public static void main(String[] args) {
        Clert clert = new Clert();
        Producer p1 = new Producer(clert);
        p1.setName("生产者1");
        p1.start();

        Consumer c1 = new Consumer(clert);
        Consumer c2 = new Consumer(clert);
        c1.setName("消费者1");
        c2.setName("消费者2");
        c1.start();
        c2.start();
    }
}
