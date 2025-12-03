<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Consumer;

use AMQPEnvelope;
use AMQPQueue;
use JardisCore\Messaging\Connection\RabbitMqConnection;
use JardisPsr\Messaging\ConsumerInterface;
use JardisPsr\Messaging\Exception\ConnectionException;
use JardisPsr\Messaging\Exception\ConsumerException;

/**
 * RabbitMQ message consumer
 *
 * Consumes messages from RabbitMQ queues with acknowledgement support
 */
class RabbitMqConsumer implements ConsumerInterface
{
    private bool $running = false;
    private ?AMQPQueue $queue = null;

    /**
     * @param RabbitMqConnection $connection RabbitMQ connection instance
     * @param string $queueName Queue name
     * @param array<string, mixed> $queueConfig Queue configuration (flags, arguments)
     */
    public function __construct(
        private readonly RabbitMqConnection $connection,
        private readonly string $queueName,
        private readonly array $queueConfig = []
    ) {
    }

    /**
     * @inheritDoc
     * @throws ConnectionException
     */
    public function consume(string $topic, callable $callback, array $options = []): void
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        $this->setupQueue($topic, $options);

        if ($this->queue === null) {
            throw new ConsumerException('Failed to setup queue');
        }

        $this->running = true;
        $flags = $options['flags'] ?? AMQP_NOPARAM;
        $timeout = $options['timeout'] ?? 1.0; // Timeout in seconds for get()
        $maxEmptyPolls = $options['max_empty_polls'] ?? 10;
        $emptyPollCount = 0;

        try {
            while ($this->running) {
                // Use get() with timeout instead of blocking consume()
                $envelope = $this->queue->get();

                if ($envelope === null) {
                    // No message available
                    $emptyPollCount++;
                    if ($emptyPollCount >= $maxEmptyPolls) {
                        // No messages for too long, stop consuming
                        break;
                    }
                    // Sleep briefly to avoid busy-waiting
                    usleep((int) ($timeout * 1000000));
                    continue;
                }

                // Reset empty poll counter when we get a message
                $emptyPollCount = 0;

                // Handle the message (envelope is AMQPEnvelope here, not null)
                $this->handleMessage($envelope, $this->queue, $callback);
            }
        } catch (\AMQPException $e) {
            // @phpstan-ignore-next-line - $this->running can be changed externally
            if ($this->running) {
                throw new ConsumerException(
                    "Failed to consume from RabbitMQ: {$e->getMessage()}",
                    previous: $e
                );
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function stop(): void
    {
        $this->running = false;

        if ($this->queue !== null) {
            try {
                $this->queue->cancel();
            } catch (\AMQPException) {
                // Ignore cancel errors
            }
            $this->queue = null;
        }
    }

    /**
     * Setup queue and bindings
     *
     * @param string $routingKey Routing key for binding
     * @param array<string, mixed> $options Options
     * @throws ConsumerException
     */
    private function setupQueue(string $routingKey, array $options): void
    {
        try {
            $channel = $this->connection->getChannel();
            $this->queue = new AMQPQueue($channel);

            $this->queue->setName($this->queueName);

            // Set queue flags (durable, exclusive, auto-delete)
            $queueFlags = $this->queueConfig['flags'] ?? $options['queue_flags'] ?? AMQP_DURABLE;
            $this->queue->setFlags($queueFlags);

            // Set queue arguments (x-message-ttl, x-max-length, etc.)
            if (isset($this->queueConfig['arguments']) && is_array($this->queueConfig['arguments'])) {
                $this->queue->setArguments($this->queueConfig['arguments']);
            }

            // Declare queue
            $this->queue->declareQueue();

            // Bind to exchange
            $exchange = $this->connection->getExchange();
            $exchangeName = $exchange->getName();
            $bindingKey = $options['binding_key'] ?? $routingKey;

            if ($exchangeName !== null && $exchangeName !== '') {
                $this->queue->bind($exchangeName, $bindingKey);
            }

            // Set QoS (prefetch count) - channel level
            if (isset($options['prefetch_count'])) {
                $channel = $this->connection->getChannel();
                $channel->qos(0, (int) $options['prefetch_count']);
            }
        } catch (\AMQPException $e) {
            throw new ConsumerException(
                "Failed to setup RabbitMQ queue: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Handle consumed message
     *
     * @param AMQPEnvelope $envelope Message envelope
     * @param AMQPQueue $queue Queue instance
     * @param callable $callback Message handler
     * @return bool True if should continue consuming, false to stop
     */
    private function handleMessage(AMQPEnvelope $envelope, AMQPQueue $queue, callable $callback): bool
    {
        $metadata = [
            'routing_key' => $envelope->getRoutingKey(),
            'delivery_tag' => $envelope->getDeliveryTag(),
            'exchange' => $envelope->getExchangeName(),
            'headers' => $envelope->getHeaders(),
            'timestamp' => $envelope->getTimestamp(),
            'content_type' => $envelope->getContentType(),
            'content_encoding' => $envelope->getContentEncoding(),
            'delivery_mode' => $envelope->getDeliveryMode(),
            'priority' => $envelope->getPriority(),
            'correlation_id' => $envelope->getCorrelationId(),
            'reply_to' => $envelope->getReplyTo(),
            'expiration' => $envelope->getExpiration(),
            'message_id' => $envelope->getMessageId(),
            'app_id' => $envelope->getAppId(),
            'user_id' => $envelope->getUserId(),
            'type' => 'rabbitmq'
        ];

        $deliveryTag = $envelope->getDeliveryTag();

        if ($deliveryTag === null) {
            error_log("Received message without delivery tag");
            return true;
        }

        try {
            $continue = $callback($envelope->getBody(), $metadata);
            // Acknowledge message
            $queue->ack($deliveryTag);

            if (!$continue) {
                // Callback returned false - stop consuming
                $this->stop();
                return false;
            }
            return true;
        } catch (\Exception $e) {
            error_log("Error handling RabbitMQ message: {$e->getMessage()}");
            // Negative acknowledge with requeue on error
            try {
                $queue->nack($deliveryTag, AMQP_REQUEUE);
            } catch (\AMQPException) {
                // Ignore nack errors
            }
            return true;
        }
    }
}
