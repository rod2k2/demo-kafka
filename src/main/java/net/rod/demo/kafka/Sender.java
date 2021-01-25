package net.rod.demo.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.rod.demo.kafka.model.UserInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Rod ,have fun with coding
 * @date 2021/1/23
 */
@Slf4j
public class Sender {

    private static Producer<String,String> producer = null;

    private static synchronized Producer<String, String> getProducer() {
        if (producer == null) {
            Properties pro = new Properties();
            pro.put("bootstrap.servers", "localhost:9092");//broker url
            pro.put("acks", "all");
            pro.put("retries", 0);
            pro.put("linger.ms", 1);
            producer = new KafkaProducer<>(pro, new StringSerializer(), new StringSerializer());
        }
        return producer;
    }

    public static void main(String[] args) {
        Producer<String, String> producer = getProducer();
        ObjectMapper mapper = new ObjectMapper();
        log.debug("start send message");
        //send just string into topic
        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> msg = new ProducerRecord<>("test", "Key" + i, "value" + i);
            producer.send(msg);
        }

        //topic for json value
        for (int i = 0; i <= 10; i++) {
            UserInfo userInfo = new UserInfo();
            userInfo.setUserId(i);
            userInfo.setUserName("User:" + i);
            ProducerRecord<String, String> msg = null;
            try {
                msg = new ProducerRecord<>("test", userInfo.getUserId().toString(), mapper.writeValueAsString(userInfo));
            } catch (JsonProcessingException e) {
                log.error(e.getMessage(), e);
            }
            producer.send(msg);
        }

        log.debug("after send message");
        producer.close();

    }
}
