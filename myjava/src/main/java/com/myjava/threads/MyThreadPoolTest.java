package com.myjava.threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 17:38
 * @Description: 线程创建方式四：线程池创建线程
 *              优势：
 *                      1、提高响应速度
 *                      2、降低资源消耗
 *                      3、便于线程管理
 *              注：开发中常用线程池方式创建线程
 *         部分属性解析：基于解析第三种优势
 *              1、corePoolSize 核心池大小
 *              2、maximumPoolSize 最大线程数
 *              3、KeepAliveTime：线程没有任务时最多保持多久时间后终止
 */
class MyThreadPool1 implements Runnable{
    @Override
    public void run() {
        for (int i = 0; i <= 100 ; i++) {
            if(i % 2 == 0){
                System.out.println(Thread.currentThread().getName()+" : "+i);
            }
        }
    }
}
class MyThreadPool2 implements Runnable{
    @Override
    public void run() {
        for (int i = 0; i <= 100 ; i++) {
            if(i % 2 != 0){
                System.out.println(Thread.currentThread().getName()+" : "+i);
            }
        }
    }
}

public class MyThreadPoolTest {
    public static void main(String[] args) {
        ThreadPoolExecutor service = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);
        //设置线程池属性
//        System.out.println(service.getClass());
        service.setCorePoolSize(15);
        service.setMaximumPoolSize(20);
//        service.setKeepAliveTime(200);
        //执行指定的线程操作，需提供实现runnable接口或callable接口实现类对象
        service.execute(new MyThreadPool1());
        service.execute(new MyThreadPool2());

//        service.submit(); //适用于callable
        //关闭线程池
        service.shutdown();
    }
}
