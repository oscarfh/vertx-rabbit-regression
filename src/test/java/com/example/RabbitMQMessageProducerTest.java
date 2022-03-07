package com.example;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.rabbitmq.RabbitMQClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import rx.Completable;
import rx.Single;

import static com.example.RabbitMQMessageProducer.MAPPER;
import static com.example.RabbitMQMessageProducer.RABBITMQ_DEFAULT_EXCHANGE_AUTO_DELETE;
import static com.example.RabbitMQMessageProducer.RABBITMQ_DEFAULT_EXCHANGE_DURABLE;
import static com.example.RabbitMQMessageProducer.RABBITMQ_DEFAULT_EXCHANGE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@ExtendWith(VertxExtension.class)
class VertxRabbitMQTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxRabbitMQTest.class);

    private static final String TEST_TOPIC = "test_topic";
    private static final String TEST_MESSAGE = "My Message";
    public static final boolean DEFAULT_RABBITMQ_QUEUE_DURABLE = false;
    public static final boolean DEFAULT_RABBITMQ_QUEUE_EXCLUSIVE = false;
    public static final boolean DEFAULT_RABBITMQ_QUEUE_AUTO_DELETE = true;

    AtomicReference<RabbitMQMessageProducer> producerReference = new AtomicReference<>();
    AtomicLong producerTimeReference = new AtomicLong();
    AtomicReference<RabbitMQClient> consumerReference = new AtomicReference<>();
    AtomicReference<Handler<AsyncResult<String>>> handlerReference = new AtomicReference<>();

    public Network network = Network.newNetwork();

    @Container
    public GenericContainer rabbitmq = new GenericContainer(DockerImageName.parse("rabbitmq:3.8.6-alpine"))
            .withExposedPorts(5672)
            .withNetwork(network);

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");
    @Container
    public ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network);

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    public void canRecoverConnectionOutage(io.vertx.core.Vertx v, VertxTestContext testContext) {
        Vertx vertx = new Vertx(v);
        createAndStartProducer(vertx);


        Handler<AsyncResult<String>> secondMessageHandler = result1 -> {
            LOGGER.info("Got another message. Connection recovery was successful.");
            vertx.cancelTimer(producerTimeReference.get());
            producerReference.get().close();
            consumerReference.get().rxStop().subscribe();
            testContext.completeNow();
        };

        Handler<AsyncResult<String>> firstMessageHandler = event -> {
            vertx.rxExecuteBlocking(f -> {
                LOGGER.info("Got a message, Shutdown rabbitmq.");
                rabbitmq.stop();
                f.complete();
            }).flatMap(a -> {
                return vertx.rxExecuteBlocking(f -> {
                    LOGGER.info("Restore RabbitMQ and wait for one more message.");
                    rabbitmq.start();
                    handlerReference.set(secondMessageHandler);
                });
            }).subscribe();
        };

        handlerReference.set(firstMessageHandler);

        Consumer<String> messageProcessor = message -> {
            //assertEquals(TEST_MESSAGE, message);
            LOGGER.warn("Received message: {}", message);
            handlerReference.get().handle(Future.succeededFuture());
        };


        createAndStartConsumer(vertx, consumerReference, messageProcessor);

        LOGGER.info("Await message from rabbitmq.");

    }

    private void createAndStartProducer(Vertx vertx) {
        Configuration configuration = new Configuration(TEST_TOPIC);

        RabbitMQOptions options = getRabbitMQOptions();

        RabbitMQClient rabbitMQClient = RabbitMQClient.create(vertx, options);
        RabbitMQMessageProducer rabbitMQMessageProducer = new RabbitMQMessageProducer(rabbitMQClient, TEST_TOPIC,
                configuration);
        rabbitMQMessageProducer.setUp().andThen(Completable.fromAction(() ->
                vertx.setPeriodic(1000, unused -> rabbitMQMessageProducer.send(TEST_MESSAGE))))
                .andThen(Completable.fromAction(() -> producerReference.set(rabbitMQMessageProducer))).subscribe();

    }

    private void createAndStartConsumer(Vertx vertx, AtomicReference<RabbitMQClient> consumerReference,
            Consumer<String> messageProcessor) {
        Configuration configuration = new Configuration(TEST_TOPIC);
        createConsumer(vertx,
                configuration,
                messageProcessor).subscribe(consumerReference::set);
    }

    private <T> Single<RabbitMQClient> createConsumer(Vertx vertx,
            Configuration configuration,
            Consumer<String> processor) {
        String topic = configuration.getTopic();
        LOGGER.info("Registering RabbitMQ message consumer for exchange {}...", topic);
        RabbitMQOptions rabbitMQOptions = getRabbitMQOptions();
        rabbitMQOptions.setAutomaticRecoveryEnabled(false);
        rabbitMQOptions.setReconnectAttempts(Integer.MAX_VALUE);
        rabbitMQOptions.setReconnectInterval(500);
        RabbitMQClient rabbitMQClient = RabbitMQClient.create(vertx, rabbitMQOptions);

        boolean durable = DEFAULT_RABBITMQ_QUEUE_DURABLE;
        boolean autoDelete = DEFAULT_RABBITMQ_QUEUE_AUTO_DELETE;
        boolean exclusive = DEFAULT_RABBITMQ_QUEUE_EXCLUSIVE;
        return rabbitMQClient.rxStart()
                .flatMap(aVoid -> rabbitMQClient.rxExchangeDeclare(topic, RABBITMQ_DEFAULT_EXCHANGE_TYPE,
                        RABBITMQ_DEFAULT_EXCHANGE_DURABLE, RABBITMQ_DEFAULT_EXCHANGE_AUTO_DELETE))
                .flatMap(aVoid -> rabbitMQClient.rxQueueDeclare(TEST_TOPIC, durable, exclusive, autoDelete))
                .flatMap(declareResult -> rabbitMQClient.rxQueueBind(TEST_TOPIC, topic, ""))
                .flatMap(aVoid -> rabbitMQClient.rxBasicConsumer(TEST_TOPIC))
                .map(rabbitMQConsumer -> rabbitMQConsumer.handler(rabbitMQMessage -> {
                    byte[] value = rabbitMQMessage.body().getBytes();
                    String decodedValue = decode(value, String.class);
                    LOGGER.debug("Received value {} of type {} on RabbitMQ message queue for exchange {}",
                            decodedValue,
                            String.class.getName(),
                            topic);
                    processor.accept(decodedValue);
                }))
                .map(rabbitMQConsumer -> rabbitMQConsumer
                        .exceptionHandler(t -> LOGGER.warn("Exception in RabbitMQ consumer", t)))
                .map(rabbitMQConsumer -> rabbitMQClient)
                .doOnSuccess(rabbitMQConsumer -> LOGGER
                        .debug("Registered RabbitMQ message consumer for exchange {}", topic));
    }

    private RabbitMQOptions getRabbitMQOptions() {
        RabbitMQOptions options = new RabbitMQOptions();

        ToxiproxyContainer.ContainerProxy proxy = toxiproxy.getProxy(rabbitmq, 5672);
        options.setHost(proxy.getContainerIpAddress());
        options.setPort(proxy.getProxyPort());
        options.setAutomaticRecoveryEnabled(true);
        options.setConnectionTimeout(1000);
        options.setNetworkRecoveryInterval(1000);
        options.setRequestedHeartbeat(1);
        return options;
    }

    public static <T> T decode(byte[] bytes, Class<T> type) {
        return (T) new String(bytes);
    }
}