package com.mykafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/22 13:01
 * @Description:
 */
public class KafkaProducerKerberos {
    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "E:\\workSpace\\realtime\\mykafka\\src\\main\\resources\\krb5.conf");
        System.setProperty("java.security.auth.login.config", "E:\\workSpace\\realtime\\mykafka\\src\\main\\resources\\kafka_client_jaas_test.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");

        Properties props  = new Properties();
        props.put("bootstrap.servers", "shd148.yonghui.cn:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("delivery.timeout.ms",30001);
        props.put("retries",1);
        props.put("sasl.mechanism", "GSSAPI");
        try {
            KafkaProducer<String, String> producerTest = new KafkaProducer<String, String>(props);

            ProducerRecord<String, String> record = new ProducerRecord("xinyuan_test_topic_153", "hello", "我来连接了");
            producerTest.send(record);
            producerTest.flush();
            producerTest.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
