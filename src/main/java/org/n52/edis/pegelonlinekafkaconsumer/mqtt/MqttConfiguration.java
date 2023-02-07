package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MqttConfiguration {

    @Autowired
    MqttMessageDeliveryMonitor monitor;

    @Bean
    @ConfigurationProperties(prefix = "edis.mqtt")
    @ConditionalOnProperty(value="edis.mqtt.enabled", havingValue = "true", matchIfMissing = false)
    public MqttPublisher mqttPublisher() {
        return new MqttPublisher(monitor);
    }

}
