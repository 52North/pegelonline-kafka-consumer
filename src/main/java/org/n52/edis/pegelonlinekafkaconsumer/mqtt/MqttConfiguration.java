package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MqttConfiguration {

    @Autowired
    MqttMessageDeliveryMonitor monitor;

    @Bean
    @ConfigurationProperties(prefix = "edis.mqtt")
    @ConditionalOnExpression("${edis.mqtt.enabled:true} == true && ${edis.mqtt.tls.tls-enabled:false} == false")
    public MqttPublisher mqttPublisher() {
        return new MqttPublisher(monitor);
    }

    @Bean
    @ConfigurationProperties(prefix = "edis.mqtt")
    @ConditionalOnExpression("${edis.mqtt.enabled:true} == true && ${edis.mqtt.tls.tls-enabled:true} == true")
    public MqttPublisher mqttTlsPublisher() {
        return new MqttTlsPublisher(monitor);
    }

}
