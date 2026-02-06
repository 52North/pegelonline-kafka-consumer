package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.resilience.annotation.EnableResilientMethods;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@EnableResilientMethods
@EnableAsync
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
