package com.mykafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/7 22:24
 * @Description:
 *              消费者消费数据测试
 */
public class MyConsumer {
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

        //消费指定分区
//        TopicPartition tp = new TopicPartition("test0", 2);
        //消费者订阅主题
        consumer.subscribe(Arrays.asList("test0"));
        //指定偏移量进行重复消费
//        consumer.seek(tp,20);

        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord record:consumerRecords){
                System.out.println(" topic="+record.topic()+
                                    " offset="+record.offset()+
                                    " key = "+record.key() +
                                    " value="+record.value() +
                                    " partition=" + record.partition()+
                                    " timestamp"+record.timestamp());
            }
            //手动提交偏移量,同步提交，每消费一批数据就进行提交偏移量，提交失败继续进行提交
            consumer.commitSync();
            //手动提交偏移量，异步提交，失败不进行提交
            //consumer.commitAsync();
        }
    }
}
