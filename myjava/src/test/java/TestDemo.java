import com.google.common.hash.BloomFilter;
import org.junit.Test;

import java.util.UUID;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/6 10:41
 * @Description:
 */
public class TestDemo {
    @Test
    public void testUUID(){
        String token = UUID.randomUUID().toString().replace("-", "");
        System.out.println(token+"\t"+token.length());
    }
    @Test
    public void testLongtime(){
        long timeout = 3L * 1000 * 1000 * 1000;
        long nowTime = System.nanoTime();
        System.out.println(timeout);
        System.out.println(nowTime);
        System.out.println(System.nanoTime());
        System.out.println(System.nanoTime() - nowTime);
        System.out.println((System.nanoTime() - nowTime) < timeout);
    }
    @Test
    public void testString(){
        String ddl = "CREATE external TABLE IF NOT EXISTS stg.ssss1( " +
                "" +
                "`id` STRING  COMMENT 'id'," +
                "`name` STRING  COMMENT 'name'," +
                "`age` STRING  COMMENT 'age' ) " +
                " COMMENT 'ssss1' " +
                " PARTITIONED BY" +
                " (" +
                "`sdt` STRING  COMMENT 'sdt' )" +
                " STORED AS PARQUET" +
                " LOCATION 'hdfs://nameservice1/yhtech/warehouse/stg/stg_test'";

        ddl.trim().replaceAll("\n","");
        System.out.println(ddl);
    }

    @Test
    public void testBloomFilter(){
        // 创建一个BloomFilter，其预计插入的个数为10，误判率大约为0.01
        BloomFilter<Person> bloomFilter = BloomFilter.create(PersonFunnel.INSTANCE, 10, 0.01);
        // 查询new Person("chen", "yahui")是否存在
        System.out.println(bloomFilter.mightContain(new Person("chen", "yahui"))); //false
        // 将new Person("chen", "yahui")对象放入BloomFilter中
        bloomFilter.put(new Person("chen","yahui"));
        // 再次查询new Person("chen", "yahui")是否存在
        System.out.println(bloomFilter.mightContain(new Person("chen", "yahui"))); //true
    }
    @Test
    public void test(){
        System.out.println("00000000000000");
    }

}
