package io.kafka.module.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String [] args) throws ExecutionException, InterruptedException {
        final String ENV_VAR_FOR_USER="<username>";
        final String ENV_VAR_FOR_PASSWORD="<password>";
//        System.out.println("Hello Kafka!!");
        log.info("Starting kafka producer....");
        Properties props = new Properties();


        props.setProperty("bootstrap.servers", "localhost:9092"); // or the appropriate address of your Kafka broker

//        Create a producer
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kprod = new KafkaProducer<>(props);
        ProducerRecord<String, String>  producerRecord = new ProducerRecord<>("topic_2", "hello its kafka on the shore");

//      send data
        kprod.send(producerRecord).get();
//        kprod.send(producerRecord);
//        flush producer
        kprod.flush();
//        close producer
        kprod.close();

    }
}
//        props.setProperty("security.protocol", "SASL_PLAINTEXT");
//        props.setProperty("sasl.mechanism", "PLAIN"); // or SCRAM depending on your setup
//        props.put("sasl.jaas.config",
//                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                        "username=\"\" " +
//                        "password=\"\";");
