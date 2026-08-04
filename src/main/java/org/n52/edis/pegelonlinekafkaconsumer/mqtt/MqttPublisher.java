package org.n52.edis.pegelonlinekafkaconsumer.mqtt;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.jspecify.annotations.NonNull;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineMqttMessage;
import org.n52.edis.pegelonlinekafkaconsumer.model.PegelonlineTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttPublisher extends AbstractMqttPublisher implements MqttCallback, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttPublisher.class);

    private static final long SHUTDOWN_AWAIT_SECONDS = 30;

    private ObjectMapper jsonMapper;

    private MqttClient mqttClient;

    private MqttConnectOptions mqttConnectOptions;

    private ThreadPoolExecutor publishExecutor;

    private final MqttMessageDeliveryMonitor monitor;

    public MqttPublisher() {
        monitor = new MqttMessageDeliveryLoggingMonitor();
    }

    public MqttPublisher(MqttMessageDeliveryMonitor monitor) {
        this.monitor = monitor;
    }

    @Async
    @Retryable(
            includes = MqttException.class,
            maxRetries = 3,
            maxRetriesString = "${edis.mqtt.initial-connect-retry-attempts}",
            maxDelay = 60000,
            maxDelayString = "${edis.mqtt.initial-connect-retry-delay}"
    )
    public void connect() throws MqttException {
        LOGGER.info("Trying to connect to MQTT broker...");
        try {
            mqttClient.connect(mqttConnectOptions);
        } catch (MqttException ex) {
            LOGGER.error("Could not connect to MQTT broker. Cause: {}", ex.getMessage());
            LOGGER.debug("MQTT command failure.", ex);
            throw ex;
        }
        LOGGER.info("Connection to MQTT broker successfully established.");
    }

    public boolean isConnected() {
        return mqttClient.isConnected();
    }

    public void publishMessage(PegelonlineMqttMessage payload, PegelonlineTopic topic) throws JacksonException {
        // Serialize on the calling (Kafka listener) thread so JacksonException still propagates.
        MqttMessage message = new MqttMessage();
        message.setPayload(jsonMapper.writeValueAsBytes(payload));
        message.setQos(getQos());
        message.setRetained(isRetained());
        String topicString = topic.asTopicString();
        // Hand the blocking network publish to the worker pool. A full queue makes the caller
        // run the publish inline (CallerRunsPolicy), throttling Kafka polling instead of dropping.
        publishExecutor.execute(() -> doPublish(topicString, message));
    }

    private void doPublish(String topicString, MqttMessage message) {
        try {
            mqttClient.publish(topicString, message);
        } catch (MqttException | RuntimeException e) {
            monitor.handleFailedMessageDelivery(message, e);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        LOGGER.warn("Connection to MQTT broker lost.", throwable);
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) {
        // We only publish messages but do not consume any. Thus, implementing this method is not needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        monitor.handleMessageDelivery(token);
    }

    @Override
    public void afterPropertiesSet() throws MqttException {
        logConfiguration();
        mqttClient = createMqttClient();
        mqttConnectOptions = createMqttConnectOptions();
        jsonMapper = createObjectMapper();
        publishExecutor = createPublishExecutor();
    }

    protected ThreadPoolExecutor createPublishExecutor() {
        // core == max so all workers are live and the bounded queue fills before CallerRuns engages.
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(@NonNull Runnable r) {
                Thread thread = new Thread(r, "mqtt-pub-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
        return new ThreadPoolExecutor(
                getWorkerThreads(),
                getWorkerThreads(),
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(getQueueCapacity()),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    protected MqttClient createMqttClient() throws MqttException {
        MqttClientPersistence persistence = createMqttClientPersistence();
        mqttClient = new MqttClient(getServerUris().get(0), createClientId(), persistence);
        mqttClient.setCallback(this);
        return mqttClient;
    }


    protected MqttClientPersistence createMqttClientPersistence() {
        if (isFilePersistenceEnabled()) {
            return new MqttDefaultFilePersistence(getFilePersistenceDirectory());
        } else {
            return new MemoryPersistence();
        }
    }

    protected MqttConnectOptions createMqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(getServerUris().toArray(new String[0]));
        return applyCommonConnectOptions(options);
    }

    /**
     * Applies the connection options shared by the plain and TLS publishers. Kept in one place so
     * settings such as {@code maxInflight} cannot be forgotten in one of the two option builders.
     */
    protected MqttConnectOptions applyCommonConnectOptions(MqttConnectOptions options) {
        options.setAutomaticReconnect(isReconnect());
        options.setCleanSession(isCleanSession());
        options.setConnectionTimeout(getConnectionTimeout());
        options.setKeepAliveInterval(getKeepAliveInterval());
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        // Must be >= worker threads (+1 for the CallerRuns thread) or Paho throws
        // REASON_CODE_MAX_INFLIGHT (32202) once several workers publish concurrently, even at QoS 0.
        options.setMaxInflight(getMaxInflight());
        if (isBasicAuthentication()) {
            options.setUserName(getUsername());
            options.setPassword(getPassword().toCharArray());
        }
        options.setCleanSession(true);
        return options;
    }

    protected String createClientId() {
        if (getClientId() == null) {
            return String.join(".", getClientIdPrefix(), UUID.randomUUID().toString());
        } else {
            return String.join(".", getClientIdPrefix(), getClientId());
        }
    }

    @Override
    public void destroy() throws Exception {
        // Drain queued publishes before disconnecting the client. The Kafka container is a
        // SmartLifecycle and is stopped before this DisposableBean runs, so no new tasks arrive here.
        if (publishExecutor != null) {
            publishExecutor.shutdown();
            try {
                if (!publishExecutor.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Publish executor did not drain within {}s; {} task(s) may be lost.",
                            SHUTDOWN_AWAIT_SECONDS, publishExecutor.getQueue().size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        mqttClient.disconnect();
    }
}
