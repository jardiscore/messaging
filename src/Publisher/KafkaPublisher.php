<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Publisher;

use JardisCore\Messaging\Connection\KafkaConnection;
use JardisPsr\Messaging\PublisherInterface;
use JardisPsr\Messaging\Exception\ConnectionException;
use JardisPsr\Messaging\Exception\PublishException;
use RdKafka\ProducerTopic;

/**
 * Kafka message publisher
 *
 * Uses Kafka producer for message publishing
 */
class KafkaPublisher implements PublisherInterface
{
    /** @var array<string, ProducerTopic> */
    private array $topics = [];

    public function __construct(
        private readonly KafkaConnection $connection
    ) {
    }

    /**
     * Publish a message to the specified topic
     *
     * @param string $topic The Kafka topic name
     * @param string $message The message payload (already serialized)
     * @param array<string, mixed> $options Publisher-specific options (partition, key)
     * @return bool True on success
     * @throws PublishException
     * @throws ConnectionException
     */
    public function publish(string $topic, string $message, array $options = []): bool
    {
        if (!$this->connection->isConnected()) {
            $this->connection->connect();
        }

        try {
            $producerTopic = $this->getOrCreateTopic($topic);

            $partition = $options['partition'] ?? RD_KAFKA_PARTITION_UA;
            $key = $options['key'] ?? null;

            $producerTopic->produce($partition, 0, $message, $key);

            return true;
        } catch (\Exception $e) {
            throw new PublishException(
                "Failed to publish message to Kafka topic '{$topic}': {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Get or create a producer topic
     */
    private function getOrCreateTopic(string $topicName): ProducerTopic
    {
        if (!isset($this->topics[$topicName])) {
            $producer = $this->connection->getClient();
            $this->topics[$topicName] = $producer->newTopic($topicName);
        }

        return $this->topics[$topicName];
    }
}
