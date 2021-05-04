package com.mykafka.test;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/3/29 10:54
 * @Description:
 */
public class TestCallBackProducer {
    public static void main(String[] args) {
        //1.配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node2:9092,node3:9092,node4:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.mykafka.test.MyPartitioner");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("test0", "hello--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null == exception){
                        System.out.println("offset" + metadata.offset()
                                            +"\t topic: \t"+metadata.topic()
                                            +"\t partition: \t"+metadata.partition()
                                            +"\t timestamp: \t"+metadata.timestamp()
                                            +"\t hashCode: \t"+metadata.hashCode()
                                            +"\t toString: \t"+metadata.toString()
                        );
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        //关闭资源
        producer.close();
    }
}
