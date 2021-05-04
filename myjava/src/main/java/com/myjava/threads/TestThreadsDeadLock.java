package com.myjava.threads;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 12:33
 * @Description: 线程死锁测试，
 *              1、不同的线程分别占用对方需要的同步资源不放弃，都在等待对方放弃自己需要的同步资源就形成了线程的死锁
 *              2、现象：不会异常，不会出现提示，所有线程处于阻塞状态，无法继续
 *              程序中应避免死锁情况发生
 */
public class TestThreadsDeadLock {
    public static void main(String[] args) {
        //两个锁对象
        StringBuffer sb1 = new StringBuffer();
        StringBuffer sb2 = new StringBuffer();

        //线程1，继承方式实现
        new Thread(){
            @Override
            public void run() {
                synchronized (sb1){
                    sb1.append("a");
                    sb2.append("1");

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (sb2){
                        sb1.append("b");
                        sb2.append("2");
                        System.out.println(sb1);
                        System.out.println(sb2);
                    }
                }
            }
        }.start();

        //线程2，实现runnable接口形式
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (sb2){
                    sb1.append("c");
                    sb2.append("3");

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    synchronized (sb1){
                        sb1.append("d");
                        sb2.append("4");
                        System.out.println(sb1);
                        System.out.println(sb2);
                    }
                }
            }
        }).start();

    }
}
