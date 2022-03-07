package com.example;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.rabbitmq.RabbitMQClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;

import static com.rabbitmq.client.BuiltinExchangeType.FANOUT;

public class RabbitMQMessageProducer {
    public static final boolean RABBITMQ_DEFAULT_EXCHANGE_DURABLE = true;
    public static final boolean RABBITMQ_DEFAULT_EXCHANGE_AUTO_DELETE = false;
    public static final String RABBITMQ_DEFAULT_EXCHANGE_TYPE = FANOUT.getType();
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessageProducer.class);

    private final RabbitMQClient rabbitMQClient;

    private final String topic;

    private final Configuration configuration;

    private boolean started;

    public RabbitMQMessageProducer(RabbitMQClient rabbitMQClient, String topic, Configuration configuration) {
        this.rabbitMQClient = rabbitMQClient;
        this.topic = topic;
        this.configuration = configuration;
    }

    public Completable setUp() {
        return rabbitMQClient.rxStart()
                .flatMap(aVoid -> rabbitMQClient.rxExchangeDeclare(topic,
                        RABBITMQ_DEFAULT_EXCHANGE_TYPE,
                        RABBITMQ_DEFAULT_EXCHANGE_DURABLE,
                        RABBITMQ_DEFAULT_EXCHANGE_AUTO_DELETE))
                .doOnSuccess(result -> {
                    LOGGER.info("Created exchange '{}': {}", topic, result);
                    started = true;
                })
                .flatMap(aVoid -> rabbitMQClient.rxConfirmSelect())
                .doOnSuccess(result -> LOGGER.info("Confirmation enabled for topic {}", topic))
                .doOnError(throwable -> LOGGER.warn("Failed to create exchange '{}'", topic, throwable))
                .toCompletable();
    }

    public void send(String message) {
        if (!started) {
            LOGGER.debug("Skipping sending of message {} to topic {}, because client is not started", message, topic);
            return;
        }

        LOGGER.debug("Sending message {} to RabbitMQ topic {}", message, topic);
        byte[] messageAsBytes = encodeAsBytes(message);

        long brokerConfirmTimeoutMs = configuration.getRabbitMQBrokerConfirmTimeoutMs();

        rabbitMQClient
                .rxBasicPublish(topic, "", Buffer.buffer(messageAsBytes))
                .flatMap(aVoid -> rabbitMQClient.rxWaitForConfirms(brokerConfirmTimeoutMs))
                .doOnSuccess(aVoid -> LOGGER.debug("Published message {} to RabbitMQ topic {}", message, topic))
                .doOnError(throwable -> LOGGER.warn("Failed to publish message {} to RabbitMQ topic {}", message, topic, throwable))
                .subscribe();
    }

    public Completable close() {
        return rabbitMQClient.rxStop().toCompletable();
    }

    public static byte[] encodeAsBytes(Object o) {
        try {
            if (o instanceof JsonObject) {
                return ((JsonObject) o).encode().getBytes(StandardCharsets.UTF_8);
            } else if (o instanceof JsonArray) {
                return ((JsonArray) o).encode().getBytes(StandardCharsets.UTF_8);
            } else {
                return MAPPER.writeValueAsBytes(o);
            }
        } catch (Exception e) {
            throw new EncodeException(e.getMessage());
        }
    }
}