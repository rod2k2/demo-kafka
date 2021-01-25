package net.rod.demo.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Rod ,have fun with coding
 * @date 2021/1/23
 */
public class Sender {

    private static Producer producer = null;

    private static synchronized Producer<String, String> getProducer() {
        if (producer == null) {
            Properties pro = new Properties();
            pro.put("bootstrap.servers", "localhost:9092");//broker url
            pro.put("acks", "all");
            pro.put("retries", 0);
            pro.put("linger.ms", 1);
//            pro.put("request.timeout.ms",1000);
//            pro.put("metadata.fetch.timeout.ms",1000);
            producer = new KafkaProducer<>(pro, new StringSerializer(), new StringSerializer());
        }
        return producer;
    }

    public static void main(String[] args) {
        Producer<String, String> producer = getProducer();
        ProducerRecord<String, String> msg = new ProducerRecord<>("test", "this is key", "Hello ,Message value is 123 ");
        System.out.println("start send message");
//        producer.send(msg, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception e) {
//                if (e != null) {
//                    e.printStackTrace();
//                }
//            }
//        });
        producer.send(msg);
        System.out.println("after send message");
        producer.close();

    }
}
