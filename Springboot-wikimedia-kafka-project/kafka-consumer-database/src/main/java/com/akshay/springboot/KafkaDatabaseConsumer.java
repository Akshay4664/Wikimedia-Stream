package com.akshay.springboot;

import com.akshay.springboot.entity.WikimediaData;
import com.akshay.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @Autowired
    public WikimediaDataRepository wikimediaDataRepository;

    @KafkaListener(topics = "wikimedia_recentChange", groupId = "myGroup")
    public void consumer(String eventMessage){
        logger.info(String.format("Event Message recieved -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        wikimediaDataRepository.save(wikimediaData);
    }
}
