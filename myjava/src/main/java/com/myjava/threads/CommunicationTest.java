package com.myjava.threads;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 14:31
 * @Description:
 */
public class CommunicationTest {
    public static void main(String[] args) {
        Communication c = new Communication();

        Thread t1 = new Thread(c);
        Thread t2 = new Thread(c);

        t1.setName("线程1");
        t2.setName("线程2");

        t1.start();
        t2.start();
    }
}

class Communication implements Runnable{
    private int number = 1 ;

    @Override
    public void run() {
        while(true){
            synchronized (this){

                notifyAll();

                if(number <= 100){

                    //线程休眠，不释放锁
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println(Thread.currentThread().getName() + " : " + number);
                    number ++ ;

                    try {
                        //阻塞当前线程，释放锁
                        wait();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else {
                    break;
                }
            }
        }
    }
}
