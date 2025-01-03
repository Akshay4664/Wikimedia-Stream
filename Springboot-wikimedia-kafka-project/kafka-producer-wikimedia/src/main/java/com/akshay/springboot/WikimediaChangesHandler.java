package com.akshay.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class WikimediaChangesHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesHandler.class);

    private KafkaTemplate<String,String> template;
    private String topic;

    public WikimediaChangesHandler(KafkaTemplate<String, String> template, String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    //whenever a new message in wikimedia this onMessage() will be triggered and then it will read that event.
    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info(String.format("event data-> %s", messageEvent.getData()));

        template.send(topic, messageEvent.getData());
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
