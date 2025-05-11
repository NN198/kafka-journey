package io.kafka.module.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String [] args) throws ExecutionException, InterruptedException {

        String groupID = "my-consumer-group";
        log.info("Starting kafka producer....");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092"); // or the appropriate address of your Kafka broker
        //        Create a producer
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupID);
        props.setProperty("auto.offset.reset", "earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //reference to main thread
        final Thread mainThread = Thread.currentThread();

//      adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("detected a shutdown, calling consumer.wakeup()");
                consumer.wakeup();

                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try{
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList("demo_java"));

            // Poll for new data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }

        }
        catch (WakeupException e)
        {
            log.info("Consumer is starting to shut down....");

        }
        catch (Exception e)
        {
            log.info("Unexpected exception");
        }
        finally
        {
            consumer.close();
            log.info("Consumer has gracefully shutdown");
        }


    }
}
