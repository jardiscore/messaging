<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Integration\Consumer;

use JardisCore\Messaging\Config\ConnectionConfig;
use JardisCore\Messaging\Consumer\KafkaConsumer;
use JardisCore\Messaging\Connection\KafkaConnection;
use JardisCore\Messaging\Publisher\KafkaPublisher;
use PHPUnit\Framework\TestCase;

class KafkaConsumerTest extends TestCase
{
    private ConnectionConfig $config;

    protected function setUp(): void
    {
        $brokers = $_ENV['KAFKA_BROKERS'] ?? 'kafka:9092';
        [$host, $port] = explode(':', $brokers);

        $this->config = new ConnectionConfig(
            host: $host,
            port: (int)$port
        );
    }

    public function testCreatesConsumer(): void
    {
        $consumer = new KafkaConsumer($this->config, 'test-group');

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testStopsConsuming(): void
    {
        $consumer = new KafkaConsumer($this->config, 'test-group');

        $consumer->stop();

        $this->assertTrue(true); // No exception thrown
    }

    public function testPublishAndConsumeMessage(): void
    {
        // Use unique topic AND group to avoid conflicts
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'test-topic-' . $uniqueSuffix;
        $group = 'test-group-' . $uniqueSuffix;

        // Publish messages
        $connection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($connection);
        $publisher->publish($topic, json_encode(['message' => 'test1']));
        $publisher->publish($topic, json_encode(['message' => 'test2']));
        $connection->disconnect();

        sleep(1); // Allow Kafka to process

        // Consume messages
        $consumer = new KafkaConsumer($this->config, $group);
        $received = [];

        // Consume with auto-stop after 2 messages
        $consumer->consume($topic, function (string $message, array $meta) use (&$received): bool {
            $received[] = json_decode($message, true);
            return count($received) < 2; // Stop after 2 messages
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertCount(2, $received);
        $this->assertSame('test1', $received[0]['message']);
        $this->assertSame('test2', $received[1]['message']);
    }

    public function testConsumerReceivesMetadata(): void
    {
        // Use unique topic AND group
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'metadata-topic-' . $uniqueSuffix;
        $group = 'metadata-group-' . $uniqueSuffix;

        // Publish a message
        $connection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($connection);
        $publisher->publish($topic, 'metadata test');
        $connection->disconnect();

        sleep(1);

        $consumer = new KafkaConsumer($this->config, $group);
        $receivedMeta = null;

        $consumer->consume($topic, function (string $message, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return false; // Stop after first
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertNotNull($receivedMeta);
        $this->assertArrayHasKey('topic', $receivedMeta);
        $this->assertArrayHasKey('partition', $receivedMeta);
        $this->assertArrayHasKey('offset', $receivedMeta);
        $this->assertArrayHasKey('type', $receivedMeta);
        $this->assertSame('kafka', $receivedMeta['type']);
        $this->assertSame($topic, $receivedMeta['topic']);
    }


    public function testMultipleMessagesInSequence(): void
    {
        $connection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($connection);

        // Publish multiple messages
        for ($i = 1; $i <= 5; $i++) {
            $publisher->publish('multi-test-topic', json_encode(['message' => "Message {$i}"]));
        }

        $connection->disconnect();

        $this->assertTrue(true); // Publishing succeeded
    }

    public function testConsumerWithCustomKafkaConfig(): void
    {
        $customConfig = [
            'auto.offset.reset' => 'latest'
        ];

        $consumer = new KafkaConsumer($this->config, 'custom-group', $customConfig);

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testConsumerStopsOnCallbackReturnFalse(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'stop-test-' . $uniqueSuffix;
        $group = 'stop-group-' . $uniqueSuffix;

        // Publish 3 messages
        $connection = new KafkaConnection($this->config);
        $publisher = new KafkaPublisher($connection);
        $publisher->publish($topic, 'Message 1');
        $publisher->publish($topic, 'Message 2');
        $publisher->publish($topic, 'Message 3');
        $connection->disconnect();

        sleep(1);

        $consumer = new KafkaConsumer($this->config, $group);
        $received = [];

        // Consume but stop after first message
        $consumer->consume($topic, function (string $message, array $meta) use (&$received): bool {
            $received[] = $message;
            return false; // Stop immediately
        }, ['timeout' => 1000, 'max_empty_polls' => 20]);

        $this->assertCount(1, $received);
    }

    public function testConsumerHandlesTimeout(): void
    {
        $uniqueSuffix = time() . '-' . rand(1000, 9999);
        $topic = 'timeout-test-' . $uniqueSuffix;
        $group = 'timeout-group-' . $uniqueSuffix;

        // Don't publish anything - test timeout behavior
        $consumer = new KafkaConsumer($this->config, $group);
        $messageCount = 0;

        // Should stop after max_empty_polls
        $consumer->consume($topic, function (string $message, array $meta) use (&$messageCount): bool {
            $messageCount++;
            return true;
        }, ['timeout' => 100, 'max_empty_polls' => 3]);

        // No messages should be received, consumer should timeout
        $this->assertSame(0, $messageCount);
    }

    public function testConsumerWithSaslAuth(): void
    {
        // Test that consumer can be created with SASL credentials
        $configWithAuth = new ConnectionConfig(
            host: $this->config->host,
            port: $this->config->port,
            username: 'testuser',
            password: 'testpass'
        );

        $consumer = new KafkaConsumer($configWithAuth, 'auth-group');

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testDestruct(): void
    {
        $consumer = new KafkaConsumer($this->config, 'destruct-group');

        // Trigger destructor
        unset($consumer);

        $this->assertTrue(true); // No exception thrown
    }
}
