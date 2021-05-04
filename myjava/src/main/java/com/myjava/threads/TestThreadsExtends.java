package com.myjava.threads;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/28 18:17
 * @Description:      线程创建方式-：继承Thread
 */
public class TestThreadsExtends {
    public static void main(String[] args) {
        MyThread m1 = new MyThread();
        MyThread m2 = new MyThread();
        m1.setName("线程1");
        m2.setName("线程2");
        m1.start();
        m2.start();
    }
}
class MyThread  extends Thread{
    private static int num = 100 ;
    //加锁，保证线程安全
    private static Object obj = new Object();
    @Override
    public void run() {
        while (true){
            synchronized (obj){

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
            }
        }
    }
}