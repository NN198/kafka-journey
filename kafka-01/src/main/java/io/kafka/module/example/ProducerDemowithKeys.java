package io.kafka.module.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemowithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemowithKeys.class.getSimpleName());
    public static void main(String [] args) throws ExecutionException, InterruptedException {
        final String ENV_VAR_FOR_USER="<username>";
        final String ENV_VAR_FOR_PASSWORD="<password>";
//        System.out.println("Hello Kafka!!");
        log.info("Kafka callback producer starting....");
        Properties props = new Properties();


        props.setProperty("bootstrap.servers", "localhost:9092"); // or the appropriate address of your Kafka broker

//        Create a producer
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kprod = new KafkaProducer<>(props);


        for(int j=0; j<2; j++)
        {
            for(int i=0; i<10; i++)
            {
                String topic = "demo_java";
                String key = "id_"+ i;
                String value = "kafka_part_" +i;
                ProducerRecord<String, String>  producerRecord = new ProducerRecord<>(topic, key, value);
                //      send data
                kprod.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e==null)
                        {
                            log.info("Received new metadata \n" +
                                    "Topic: " +recordMetadata.topic() +
                                    "Partition: " +recordMetadata.partition() +
                                    "Offset: " +recordMetadata.offset() +
                                    "Timestamp: " +recordMetadata.timestamp()
                            );
                        }
                        else
                        {
                            log.error("error while producing  metadata", e);
                        }
                    }
                });

            }

        }
        // After all sends complete:
        kprod.flush();
        kprod.close();

    }
}
//        props.setProperty("security.protocol", "SASL_PLAINTEXT");
//        props.setProperty("sasl.mechanism", "PLAIN"); // or SCRAM depending on your setup
//        props.put("sasl.jaas.config",
//                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
//                        "username=\"\" " +
//                        "password=\"\";");
