package net.rod.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.*;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Rod ,have fun with coding
 * @date 2021/1/26
 */
public class Consumer {

    public static void main(String[] args) {

        //init consumer
        Properties prop = new Properties();
        prop.put("bootstrap.servers","localhost:9092");
        prop.put("group.id","test-group-1");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop,new StringDeserializer(),new StringDeserializer());
        consumer.subscribe(Collections.singletonList("test"));

        try{
            while(true){
                ConsumerRecords<String, String> msgs  = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord msg : msgs){
                    System.out.println("data got at"+ LocalDateTime.ofInstant(Instant.ofEpochSecond(msg.timestamp()),ZoneId.systemDefault())
                            +"timestamp type is " + msg.timestampType()
                            +"key =" +  msg.key()+ "value=" + msg.value());
                }
            }
        }finally{
            consumer.close();
        }

    }
}
