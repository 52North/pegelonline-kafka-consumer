package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineTopic;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the decoupling of the Kafka listener thread from the blocking MQTT publish: the worker
 * pool drains a bounded queue, backpressure runs publish inline instead of dropping them, and
 * {@code destroy()} drains the queue before disconnecting. No MQTT broker is required.
 */
class MqttPublisherTest {

    private PegelonlineTopic topic() {
        return new PegelonlineTopic("edis", "water", "state", "region", "agency", "uuid", "W");
    }

    private MqttPublisher testPublisher(MqttClient client, ObjectMapper mapper, int workers, int queue) {
        MqttPublisher publisher = new MqttPublisher(mock(MqttMessageDeliveryMonitor.class)) {
            @Override
            protected MqttClient createMqttClient() {
                return client;
            }

            @Override
            protected ObjectMapper createObjectMapper() {
                return mapper;
            }
        };
        publisher.setServerUris(List.of("tcp://localhost:1883"));
        publisher.setWorkerThreads(workers);
        publisher.setQueueCapacity(queue);
        publisher.setMaxInflight(workers + 1);
        return publisher;
    }

    @Test
    void publishesEveryMessageAndDrainsBeforeDisconnect() throws Exception {
        MqttClient client = mock(MqttClient.class);
        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.writeValueAsBytes(any())).thenReturn(new byte[]{1});

        AtomicInteger published = new AtomicInteger();
        doAnswer(inv -> {
            published.incrementAndGet();
            return null;
        }).when(client).publish(anyString(), any(MqttMessage.class));

        MqttPublisher publisher = testPublisher(client, mapper, 2, 5);
        publisher.afterPropertiesSet();

        int total = 100;
        for (int i = 0; i < total; i++) {
            publisher.publishMessage(null, topic());
        }
        publisher.destroy();

        // destroy() awaits termination, so every enqueued publish must have completed and none dropped.
        assertThat(published.get()).isEqualTo(total);
        verify(client).disconnect();
    }

    @Test
    void backpressureRunsPublishesOnTheCallerThread() throws Exception {
        MqttClient client = mock(MqttClient.class);
        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.writeValueAsBytes(any())).thenReturn(new byte[]{1});

        Set<String> publishThreads = ConcurrentHashMap.newKeySet();
        AtomicInteger published = new AtomicInteger();
        doAnswer(inv -> {
            // Small delay so the single worker + 1-slot queue cannot keep up with rapid submits,
            // forcing CallerRunsPolicy to execute publishes on the submitting thread.
            Thread.sleep(20);
            publishThreads.add(Thread.currentThread().getName());
            published.incrementAndGet();
            return null;
        }).when(client).publish(anyString(), any(MqttMessage.class));

        MqttPublisher publisher = testPublisher(client, mapper, 1, 1);
        publisher.afterPropertiesSet();

        String callerThread = Thread.currentThread().getName();
        int total = 30;
        for (int i = 0; i < total; i++) {
            publisher.publishMessage(null, topic());
        }
        publisher.destroy();

        assertThat(published.get()).isEqualTo(total);
        // Backpressure engaged: at least some publishes ran inline on the caller (Kafka listener) thread.
        assertThat(publishThreads).contains(callerThread);
    }
}
