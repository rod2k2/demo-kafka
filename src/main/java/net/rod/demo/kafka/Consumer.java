package net.rod.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Rod ,have fun with coding
 * @date 2021/1/26
 */
public class Consumer {

    public static void main(String[] args) {

        //init consumer1
        Properties prop = new Properties();
        prop.put("bootstrap.servers","localhost:9092");
        prop.put("group.id","test-group-1");
        KafkaConsumer<String,String> consumer1 = new KafkaConsumer<String, String>(prop,new StringDeserializer(),new StringDeserializer());
        consumer1.subscribe(Collections.singletonList("test"));

        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");
        prop2.put("group.id","test-group-2");
        KafkaConsumer<String,String> consumer2 = new KafkaConsumer<String, String>(prop2,new StringDeserializer(),new StringDeserializer());
        consumer2.subscribe(Collections.singletonList("test"));

        //lets see two consumer1 group to consume message
        ExecutorService es = Executors.newFixedThreadPool(4);
        es.submit(consumeMessage(consumer1));
        es.submit(consumeMessage(consumer1));
        es.submit(consumeMessage(consumer2));

    }

    private static Runnable consumeMessage(KafkaConsumer consumer){
       return () -> {
           try{
               while(true){
                   ConsumerRecords<String, String> msgs  = consumer.poll(Duration.ofMillis(1000));
                   for(ConsumerRecord msg : msgs){
                       System.out.println("My name is "+ consumer.groupMetadata().groupId()+consumer.groupMetadata().memberId()+" Received ====> data got "
                               +"key =" +  msg.key()+ "value=" + msg.value());
                   }
               }
           }finally{
               consumer.close();
           }
       };
    }
}
