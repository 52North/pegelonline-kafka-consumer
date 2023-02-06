package org.n52.edis.pegelonlinekafkaconsumer.controller;

import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineKafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    private final Logger logger = LoggerFactory.getLogger(MessageController.class);

    @Autowired
    private KafkaTemplate<Object, Object> template;

    @PostMapping(path = "/send/messages")
    public void sendFoo(@RequestBody PegelonlineKafkaMessage message) {
        logger.debug("Received new PEGELONLINE message: {}", message);
        template.send("de.itzbund.pegelonline", message);
    }

}
