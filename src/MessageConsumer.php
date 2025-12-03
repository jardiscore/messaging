<?php

declare(strict_types=1);

namespace JardisCore\Messaging;

use JardisCore\Messaging\Config\ConnectionConfig;
use JardisCore\Messaging\Connection\RabbitMqConnection;
use JardisCore\Messaging\Connection\RedisConnection;
use JardisCore\Messaging\Consumer\KafkaConsumer;
use JardisCore\Messaging\Consumer\RabbitMqConsumer;
use JardisCore\Messaging\Consumer\RedisConsumer;
use JardisPsr\Messaging\ConsumerInterface;
use JardisPsr\Messaging\MessageConsumerInterface;
use JardisPsr\Messaging\MessageHandlerInterface;
use JardisPsr\Messaging\Exception\ConsumerException;

/**
 * Main message consumer class
 *
 * Provides a unified interface for consuming messages from different
 * message brokers (Redis, Kafka, RabbitMQ) with layered fallback support
 *
 * New fluent API:
 * $consumer = (new MessageConsumer())
 *     ->setRedis('localhost')
 *     ->setKafka('kafka:9092', 'group-id')
 *     ->consume('topic', $handler);
 *
 * Legacy API (still supported):
 * $consumer = new MessageConsumer($consumerInterface);
 */
class MessageConsumer implements MessageConsumerInterface
{
    private bool $autoDeserialize = true;

    /** @var array<array{type: string, consumer: ConsumerInterface, priority: int}> */
    private array $consumers = [];

    /**
     * @param ConsumerInterface|null $consumer Legacy: single consumer (optional)
     * @param bool $autoDeserialize Automatically deserialize JSON messages (default: true)
     */
    public function __construct(
        ?ConsumerInterface $consumer = null,
        bool $autoDeserialize = true
    ) {
        $this->autoDeserialize = $autoDeserialize;

        // Legacy support: single consumer
        if ($consumer !== null) {
            $this->consumers[] = [
                'type' => 'legacy',
                'consumer' => $consumer,
                'priority' => 0
            ];
        }
    }

    /**
     * Configure Redis consumer
     *
     * @param string $host Redis host
     * @param int $port Redis port
     * @param string|null $password Redis password
     * @param array<string, mixed> $options Additional options (useStreams, etc.)
     * @param int $priority Layer priority (lower = tried first)
     * @return self
     */
    public function setRedis(
        string $host,
        int $port = 6379,
        ?string $password = null,
        array $options = [],
        int $priority = 0
    ): self {
        $config = new ConnectionConfig(
            host: $host,
            port: $port,
            password: $password,
            options: $options
        );

        $connection = new RedisConnection($config);
        $useStreams = $options['useStreams'] ?? true;

        $this->consumers[] = [
            'type' => 'redis',
            'consumer' => new RedisConsumer($connection, $useStreams),
            'priority' => $priority
        ];

        $this->sortConsumersByPriority();

        return $this;
    }

    /**
     * Configure Kafka consumer
     *
     * @param string $brokers Kafka brokers (e.g., 'kafka:9092')
     * @param string $groupId Consumer group ID
     * @param string|null $username SASL username
     * @param string|null $password SASL password
     * @param array<string, mixed> $options Additional Kafka options
     * @param int $priority Layer priority (lower = tried first)
     * @return self
     */
    public function setKafka(
        string $brokers,
        string $groupId,
        ?string $username = null,
        ?string $password = null,
        array $options = [],
        int $priority = 1
    ): self {
        // Parse brokers string
        $parts = explode(':', $brokers);
        $host = $parts[0];
        $port = isset($parts[1]) ? (int) $parts[1] : 9092;

        $config = new ConnectionConfig(
            host: $host,
            port: $port,
            username: $username,
            password: $password,
            options: $options
        );

        $this->consumers[] = [
            'type' => 'kafka',
            'consumer' => new KafkaConsumer($config, $groupId, $options),
            'priority' => $priority
        ];

        $this->sortConsumersByPriority();

        return $this;
    }

    /**
     * Configure RabbitMQ consumer
     *
     * @param string $host RabbitMQ host
     * @param string $queueName Queue name
     * @param int $port RabbitMQ port
     * @param string $username RabbitMQ username
     * @param string $password RabbitMQ password
     * @param array<string, mixed> $options Additional options
     * @param int $priority Layer priority (lower = tried first)
     * @return self
     */
    public function setRabbitMq(
        string $host,
        string $queueName,
        int $port = 5672,
        string $username = 'guest',
        string $password = 'guest',
        array $options = [],
        int $priority = 2
    ): self {
        $config = new ConnectionConfig(
            host: $host,
            port: $port,
            username: $username,
            password: $password,
            options: $options
        );

        $connection = new RabbitMqConnection($config);

        $this->consumers[] = [
            'type' => 'rabbitmq',
            'consumer' => new RabbitMqConsumer($connection, $queueName, $options),
            'priority' => $priority
        ];

        $this->sortConsumersByPriority();

        return $this;
    }

    /**
     * Start consuming messages with a handler
     *
     * Tries each configured consumer in priority order (fallback on failure)
     *
     * @param string $topic The topic, channel or queue name
     * @param MessageHandlerInterface $handler Message handler
     * @param array<string, mixed> $options Consumer-specific options
     * @throws ConsumerException if no consumers configured or all fail
     */
    public function consume(string $topic, MessageHandlerInterface $handler, array $options = []): void
    {
        if (empty($this->consumers)) {
            throw new ConsumerException(
                'No consumers configured. Call setRedis(), setKafka(), or setRabbitMq() first, ' .
                'or use the legacy constructor with a ConsumerInterface.'
            );
        }

        $callback = function (string $rawMessage, array $metadata) use ($handler): bool {
            $message = $this->autoDeserialize ? $this->deserialize($rawMessage) : $rawMessage;
            return $handler->handle($message, $metadata);
        };

        $errors = [];

        // Try each consumer in priority order
        foreach ($this->consumers as $layer) {
            try {
                $layer['consumer']->consume($topic, $callback, $options);
                return; // Success
            } catch (\Exception $e) {
                $errors[] = "{$layer['type']}: {$e->getMessage()}";
                // Continue to next layer
            }
        }

        // All layers failed
        throw new ConsumerException(
            'All consumer layers failed. Errors: ' . implode(' | ', $errors)
        );
    }

    /**
     * Stop consuming messages
     */
    public function stop(): void
    {
        foreach ($this->consumers as $layer) {
            $layer['consumer']->stop();
        }
    }

    /**
     * Enable or disable auto-deserialization
     *
     * @param bool $enabled
     * @return self
     */
    public function autoDeserialize(bool $enabled = true): self
    {
        $this->autoDeserialize = $enabled;
        return $this;
    }

    /**
     * Try to deserialize JSON, fallback to raw string
     *
     * @param string $raw Raw message
     * @return string|array<mixed> Deserialized message or raw string
     */
    private function deserialize(string $raw): string|array
    {
        if ($raw === '') {
            return $raw;
        }

        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            return is_array($decoded) ? $decoded : $raw;
        } catch (\JsonException) {
            return $raw;
        }
    }

    /**
     * Sort consumers by priority (lower priority = tried first)
     */
    private function sortConsumersByPriority(): void
    {
        usort($this->consumers, fn($a, $b) => $a['priority'] <=> $b['priority']);
    }
}
