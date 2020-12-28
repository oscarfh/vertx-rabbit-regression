package com.example;

import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class RabbitMqConsumer {
    private RabbitMQClient client;

    public RabbitMqConsumer(Vertx vertx) {
        RabbitMQOptions config = new RabbitMQOptions();
        // full amqp uri
        config.setUri("amqp://xvjvsrrc:VbuL1atClKt7zVNQha0bnnScbNvGiqgb@moose.rmq.cloudamqp.com/xvjvsrrc");
        client = RabbitMQClient.create(vertx, config);
    }

    public void listen() {

        // Connect
        client.start(asyncResult -> {
            if (asyncResult.succeeded()) {
                System.out.println("RabbitMQ successfully connected!");
            } else {
                System.out.println("Fail to connect to RabbitMQ " + asyncResult.cause().getMessage());
            }
        });
    }
}
