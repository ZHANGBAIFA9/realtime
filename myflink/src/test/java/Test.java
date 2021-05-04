import java.util.ArrayList;
import java.util.List;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/18 11:19
 * @Description:
 *          两个有序的整形数组，求两个数组的交集，假设数组长度为n，要求空间复杂度o(n) 时间复杂度o（n）
 */
public class Test {
    public static void main(String[] args) {
        int[] arr1 = {1 , 2 , 3 , 4 , 6} ;
        int[] arr2 = {2 , 4 , 7 , 8 } ;
        ArrayList<Integer> arrayLists = commonSet(arr1, arr2);
        for(int arrayList:arrayLists ){
            System.out.println(arrayList + "\t");
        }
    }

    private static ArrayList commonSet(int[] arr1, int[] arr2) {

        ArrayList<Integer> list = new ArrayList<Integer>() ;
        for(int i = 0 ; i < arr1.length ; i++){
            for( int j = 0 ; j < arr2.length ; j++){
                if(arr1[i] == arr2[j]){
                    list.add(arr1[i]);
                }
            }
        }
        return list ;
    }
}
