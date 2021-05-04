package com.myjava.threads;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 15:32
 * @Description:
 *              线程创建方式三：实现Callable接口 ---> jdk5.0提供
 */
//1、实现callable接口
class TestCallable implements Callable {
    //2、实现call方法，将此线程需要执行的操作声明在call()中
    @Override
    public Object call() throws Exception {
        int sum = 0 ;
        for (int i = 1; i <= 100; i++) {
            if(i % 2 == 0){
                System.out.println(Thread.currentThread().getName()+":"+i);
                sum += i ;
            }
        }
        return sum ;
    }
}
public class TestCallableImplements {
    public static void main(String[] args) {
        //3、创建callable实现类对象
        TestCallable tc = new TestCallable();
        //4、借助FutureTask类，将此callable实现类对象传到FutureTask构造器中，去创建FutureTask对象
        FutureTask futureTask = new FutureTask(tc);
        //5、将futureTask传递到Thread中，通过Thread类去启动，---> FutureTask ---> RunnableFuture ---> extends Runnable
        new Thread(futureTask).start();

        try {
            //6、 get方法返回值即为futureTask的构造器参数callable实现重写的call方法的返回值
            Object sum = futureTask.get();
            System.out.println("总和为："+sum);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}

