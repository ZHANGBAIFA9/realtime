package com.myjava.threads;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 12:45
 * @Description:
 */
public class TestRunnableImplements {
    public static void main(String[] args) {
        MyRunnable m = new MyRunnable() ;
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
class MyRunnable implements Runnable{
    private int num = 100 ;
    @Override
    public void run() {
        while (true){
            synchronized (MyRunnable.class){
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
