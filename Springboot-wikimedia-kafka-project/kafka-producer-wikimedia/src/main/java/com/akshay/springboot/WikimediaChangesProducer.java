package com.akshay.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String,String> template;

    public WikimediaChangesProducer(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    public void sendMessage() throws InterruptedException {
        String topic = "wikimedia_recentChange";


        //to read real time stream data from wikimedia, we use event source.
        EventHandler eventHandler = new WikimediaChangesHandler(template,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        //now we will need to create an event source which will basically connect to the wikimedia source that will
        //read all the data.
        EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));
        EventSource eventSource = builder.build();

        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }
}
