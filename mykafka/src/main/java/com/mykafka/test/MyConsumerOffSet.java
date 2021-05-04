package com.mykafka.test;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/30 12:54
 * @Description:
 */
public class MyConsumerOffSet {

    private static Map<TopicPartition,Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {

        Properties props = new Properties();
        //指定服务器
        props.put("bootstrap.servers","node2:9092,node3:9092,node4:9092") ;
        //关闭自动提交
        props.put("enable.auto.commit","false") ;
        //props.put("auto.commit.interval.ms","1000");//启用自动提交设置提交时间间隔
        // key - value 反序列化
        props.put("key.deserializer",StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        //指定消费者组
//        props.put(ConsumerConfig.GROUP_ID_CONFIG,"1210");
        props.put("group.id","1210");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("second"), new ConsumerRebalanceListener() {
            //在rebalance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                conmmitOffset(currentOffset);
            }
            //在rebalance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition: partitions){
                    //定位到最近提交得offset进行消费
                    consumer.seek(partition,getOffset(partition));
                }
            }
        });
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord consumerRecord : consumerRecords){
                System.out.printf("offset = %d , key = %s ,value = %s%n",consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
                currentOffset.put(new TopicPartition(consumerRecord.topic(),consumerRecord.partition()),consumerRecord.offset());
            }
        }

    }

    //获取某分区得最新offset
    private static long getOffset(TopicPartition partition){
        return 0;
    }
    //提交该消费者得所有分区得offset
    private static void conmmitOffset(Map<TopicPartition,Long> currentOffset){

    }
}
