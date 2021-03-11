package com.myjava.suanfa.sort;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/10 16:20
 * @Description:
 *              冒泡排序算法,cr
 */
public class MaoPaoSort {
    public static void main(String[] args) {
        int[] arr = {9,2 , 4,5 , 7 , 8 ,1} ;
        int[] desc = sortBy(arr);
        for(int des: desc){
            System.out.print(des+"\t");
        }
    }
    public static int[] sortBy(int[] arr){
        int len = arr.length ;
        boolean flag ;
        for(int i = 0 ; i < len ; i++){
            flag = false ;
            for(int j = 0 ; j < len -i - 1 ; j++) {
                if(arr[j] > arr[j+1]){
                    int tmp = arr[j] ;
                    arr[j] = arr[j+1] ;
                    arr[j+1] = tmp ;
                    flag = true ;
                }
            }
            if(!flag){ break ;}
        }
        return arr ;
    }
}






