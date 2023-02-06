package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MqttConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "edis.mqtt")
    public MqttPublisher mqttPublisher() {
        return new MqttPublisher(new MqttMessageDeliveryLoggingMonitor());
    }

}
