package com.myjava.threads;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 13:32
 * @Description:
 */
public class TestLock {
    public static void main(String[] args) {
        MyLock m = new MyLock();
        Thread t1 = new Thread(m);
        Thread t2 = new Thread(m);
        Thread t3 = new Thread(m);

        t1.setName("线程1");
        t2.setName("线程2");
        t3.setName("线程3");

        t1.start();
        t2.start();
        t3.start();
    }
}
class MyLock implements Runnable{
    private int num = 100 ;
    //1、实例化 ReentrantLock
    private ReentrantLock lock = new ReentrantLock();
    @Override
    public void run() {
        while(true){
            try {
                //2、调用lock进行锁定
                lock.lock();
                if(num > 0){
//                try {
//                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                    System.out.println(Thread.currentThread().getName()+"****************"+ num );
                    num-- ;
                }else{
                    break;
                }
            }finally {
                //3、释放锁
                lock.unlock();
            }
        }
    }
}
