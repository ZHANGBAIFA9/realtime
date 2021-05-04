package com.mykafka.test;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/29 23:15
 * @Description:
 */
public class MyInterceptor implements ProducerInterceptor<String,String> {
    //onsend 方法
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return null;
    }
    // ack 方法
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }
    //关闭
    @Override
    public void close() {

    }
    //配置
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
