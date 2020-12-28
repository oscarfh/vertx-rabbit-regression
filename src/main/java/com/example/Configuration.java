package com.example;

public class Configuration {
    public static final long DEFAULT_RABBITMQ_BROKER_CONFIRM_TIMEOUT = 5000;

    private long rabbitMQBrokerConfirmTimeoutMs = DEFAULT_RABBITMQ_BROKER_CONFIRM_TIMEOUT;
    private String topic;

    public Configuration(String topic) {
        this.topic = topic;
    }

    public long getRabbitMQBrokerConfirmTimeoutMs() {
        return rabbitMQBrokerConfirmTimeoutMs;
    }

    public void setRabbitMQBrokerConfirmTimeoutMs(long rabbitMQBrokerConfirmTimeoutMs) {
        this.rabbitMQBrokerConfirmTimeoutMs = rabbitMQBrokerConfirmTimeoutMs;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
