package com.mykafka.consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/25 17:39
 * @Description:
 */
public class KafkaConsumerKerberos {
    public static void main(String[] args) {
        //在本地中设置JAAS，也可以通过-D方式传入，conf文件里面需要配置keytab文件的位置
        System.setProperty("java.security.auth.login.config", "E:\\workSpace\\realtime\\mykafka\\src\\main\\resources\\kafka_client_jaas_test.conf");
        System.setProperty("java.security.krb5.conf", "E:\\workSpace\\realtime\\mykafka\\src\\main\\resources\\krb5.conf");
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "shd148.yonghui.cn:9092");
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, "group122222");
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.put("security.protocol", "SASL_PLAINTEXT");
        consumerProp.put("sasl.mechanism", "GSSAPI");
        consumerProp.put("sasl.kerberos.service.name", "kafka");
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProp);
        consumer.subscribe(Arrays.asList("xinyuan_test_topic_153"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
