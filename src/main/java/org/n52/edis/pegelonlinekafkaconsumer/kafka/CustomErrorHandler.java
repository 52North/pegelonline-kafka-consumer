package org.n52.edis.pegelonlinekafkaconsumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

public class CustomErrorHandler implements CommonErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PegelonlineCosumer.class);


    @Override
    public boolean remainingRecords() {
        return CommonErrorHandler.super.remainingRecords();
    }

    @Override
    public void handleRecord(Exception ex, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, MessageListenerContainer container) {
        if(ex.getCause() instanceof DeserializationException) {
            DeserializationException desEx = ((DeserializationException)ex.getCause());
            String failedMessage = new String(desEx.getData());
            LOGGER.error("Can not deserialize message '{}' on topic '{}'.", failedMessage, record.topic());
            LOGGER.debug("Deserialization error.", desEx);
        } else {
            LOGGER.error("Unexpected error while consuming message. Cause: {}", ex.getMessage());
            LOGGER.debug("Consuming message error.", ex);
        }
    }

}
