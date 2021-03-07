package com.mykafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/7 17:35
 * @Description:
 *          异步发送消息测试
 */
public class MyProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "node2:9092,node3:9092,node4:9092");
        // all=-1 所有follower参与应答，1->leader 应答， 0 -> 不需要应答
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小，默认32m
        props.put("buffer.memory", 33554432);
        // key， value 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String , String>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }
}
