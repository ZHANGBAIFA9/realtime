package com.myjava.designModel;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/4 12:59
 * @Description: 懒汉式，单例模式样例，线程安全
 */
public class SingleTestL {
    public static void main(String[] args) {
        Bank b1 = Bank.getInstance();
        Bank b2 = Bank.getInstance();
        System.out.println(b1 == b2);
    }
}
class Bank{
    //
    private Bank(){}
    private volatile static Bank instance = null ;
    //线程不安全，多个线程同时进来
//    public static Bank getInstance(){
//        if(instance == null){
//            instance = new Bank() ;
//        }
//        return instance ;
//    }
    //线程安全，第一if提高效率，第二if保证只实例化一次
    public static Bank getInstance(){
        if(null == instance){
            synchronized (Bank.class){
                if(null == instance){
                    instance = new Bank() ;
                }
            }
        }
        return instance ;
    }
}
