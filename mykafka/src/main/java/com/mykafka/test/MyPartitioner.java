package com.mykafka.test;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/29 11:10
 * @Description:
 *              自定义kafka分区类,生产者参数中指定分区类
 */
public class MyPartitioner implements Partitioner {
    //重写分区函数
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
//        return key.toString().hashCode() % cluster.availablePartitionsForTopic(topic).size();
        return 1 ;
    }

    //关闭资源
    @Override
    public void close() {

    }
    // 读取配置信息
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
