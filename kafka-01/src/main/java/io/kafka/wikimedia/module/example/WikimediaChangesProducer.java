package io.kafka.wikimedia.module.example;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

public class WikimediaChangesProducer {

    public static void main(String[] args) {

        log.info("Starting kafka producer....");

        Properties props = new Properties();


        props.setProperty("bootstrap.servers", "localhost:9092"); // or the appropriate address of your Kafka broker

//        Create a producer
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        String topic = "wikipedia.recentchange";
        EventHandler eventHandler = TODO;
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder bldr = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = bldr.build();

        eventSource.start();


    }
}
