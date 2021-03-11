package com.myjava.suanfa.sort;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/10 16:45
 * @Description:
 *              选择排序
 */
public class XuanZeSort {
    public static void main(String[] args) {
        int[] arr = {2,3,8,65,4,6,4163,41,3,4,3,1,45,3} ;
        int[] descs = sortBy(arr);
        for(int desc:descs){
            System.out.print(desc+"\t");
        }
    }
    public static int[] sortBy(int[] arr){
        int len = arr.length ;
        for(int i = 0 ; i < len - 1 ; i++){
            int min = i ;
            for(int j = i + 1 ; j < len  ; j++){
                if(arr[min] > arr[j]){
                    min = j ;
                }
            }
            if(min != i){
                int tmp = arr[i] ;
                arr[i] = arr[min] ;
                arr[min] = tmp ;
            }
        }
        return arr ;
    }
}
