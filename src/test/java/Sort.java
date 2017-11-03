import java.util.ArrayList;
import java.util.List;

/**
 * Created by lta on 2017/8/9.
 */
public class Sort {

    /**
     * @param a 待排序数组元素
     * @param step 步长(桶的宽度/区间),具体长度可根据情况设定
     * @return 桶的位置/索引
     */
    private int indexFor(int a,int step){
        return a/step;
    }
    public void bucketSort(int []arr){

        int max=arr[0],min=arr[0];
        for (int a:arr) {
            if (max<a)
                max=a;
            if (min>a)
                min=a;
        }
        //该值也可根据实际情况选择
        int bucketNum=max/10-min/10+1;
        List buckList=new ArrayList<List<Integer>>();
        //create bucket
        for (int i=1;i<=bucketNum;i++){
            buckList.add(new ArrayList<Integer>());
        }
        //push into the bucket
        for (int i=0;i<arr.length;i++){
            int index=indexFor(arr[i],10);
            ((ArrayList<Integer>)buckList.get(index)).add(arr[i]);
        }
        ArrayList<Integer> bucket=null;
        int index=0;
        for (int i=0;i<bucketNum;i++){
            bucket=(ArrayList<Integer>)buckList.get(i);
            insertSort(bucket);
            for (int k : bucket) {
                arr[index++]=k;
            }
        }

    }
    //把桶内元素插入排序
    private void insertSort(List<Integer> bucket){
        for (int i=1;i<bucket.size();i++){
            int temp=bucket.get(i);
            int j=i-1;
            for (; j>=0 && bucket.get(j)>temp;j--){
                bucket.set(j+1,bucket.get(j));
            }
            bucket.set(j+1,temp);
        }
    }

    public static void main(String[] args) {

        Sort s=new Sort();
        int [] a=new int[]{1,5,4,3,4,6,42,8,6,9,5,3,4,10,54,54,64};
        s.bucketSort(a);
        for (int i : a) {
            System.out.println(i);
        }
    }
}
