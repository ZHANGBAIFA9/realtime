package com.myjava.designModel;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/7 19:40
 * @Description: 单例设计模式之饿汉式
 */
public class SingleTestE {
    public static void main(String[] args) {
        BankL instance1 = BankL.getInstance();
        BankL instance2 = BankL.getInstance();
        System.out.println(instance1 == instance2);
    }
}
class BankL{
    private BankL(){}
    private volatile static BankL instance = new BankL() ;

    public static BankL getInstance(){
        return instance ;
    }
}