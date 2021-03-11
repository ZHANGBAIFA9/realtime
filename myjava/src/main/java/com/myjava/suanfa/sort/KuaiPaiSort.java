package com.myjava.suanfa.sort;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/11 16:57
 * @Description:
 */
public class KuaiPaiSort {
    public static void main(String[] args) {
        int[] arr = { 3 , 2 , 6 , 8 , 9 ,10 , 1 } ;
        sortBy(arr , 0 , arr.length-1);
        for(int ar:arr){
            System.out.print( ar + "\t");
        }

    }
    public static void sortBy(int[] arr , int left , int rigth){
        //
        if(arr == null || arr.length == 0){
            return ;
        }
        if(left > rigth){
            return;
        }
        int key = arr[left] ;
        // 左边索引
        int l = left ;
        // 右边索引
        int r = rigth ;
        //左边 ！= 右边进入循环
        while(l != r){

            while(arr[r] >= key && l < r){
                r-- ;
            }

            while(arr[l]<key && l < r){
                l++ ;
            }
            if(l < r){
                int tmp = arr[l] ;
                arr[l] = arr[r] ;
                arr[r] = tmp ;
            }
        }
        arr[left] = arr[l] ;
        arr[l] = key ;
        sortBy(arr , left , l - 1);
        sortBy(arr , l+1 , rigth);

    }
}
