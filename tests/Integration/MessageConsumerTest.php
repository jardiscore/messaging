<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Integration;

use JardisCore\Messaging\Config\ConnectionConfig;
use JardisCore\Messaging\Connection\RedisConnection;
use JardisCore\Messaging\Consumer\RedisConsumer;
use JardisCore\Messaging\Handler\CallbackHandler;
use JardisCore\Messaging\MessageConsumer;
use JardisCore\Messaging\Publisher\RedisPublisher;
use PHPUnit\Framework\TestCase;

class MessageConsumerTest extends TestCase
{
    private ConnectionConfig $config;
    private RedisConnection $connection;

    protected function setUp(): void
    {
        $this->config = new ConnectionConfig(
            host: $_ENV['REDIS_HOST'] ?? 'redis',
            port: 6379
        );

        $this->connection = new RedisConnection($this->config);
    }

    protected function tearDown(): void
    {
        $this->connection->disconnect();
    }

    public function testCreatesMessageConsumer(): void
    {
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $this->assertInstanceOf(MessageConsumer::class, $messageConsumer);
    }


    public function testStopsConsuming(): void
    {
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $messageConsumer->stop();

        $this->assertTrue(true); // No exception thrown
    }

    public function testAutoDeserializesJsonMessage(): void
    {
        // Publish JSON message
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('json.auto.stream', json_encode(['user' => 'John', 'action' => 'login']));

        // Setup consumer with auto-deserialization
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer, autoDeserialize: true);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false; // Stop after first
        });

        $messageConsumer->consume('json.auto.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsArray($receivedMessage);
        $this->assertSame('John', $receivedMessage['user']);
        $this->assertSame('login', $receivedMessage['action']);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('json.auto.stream');
    }

    public function testWithoutAutoDeserialization(): void
    {
        // Publish JSON message
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $testData = json_encode(['data' => 'test']);
        $publisher->publish('raw.noauto.stream', $testData);

        // Setup consumer without auto-deserialization
        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer, autoDeserialize: false);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('raw.noauto.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsString($receivedMessage);
        $this->assertSame($testData, $receivedMessage);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('raw.noauto.stream');
    }

    public function testHandlerReceivesMetadata(): void
    {
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('meta.handler.stream', 'test message');

        $receivedMeta = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMeta): bool {
            $receivedMeta = $meta;
            return false;
        });

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer);

        $messageConsumer->consume('meta.handler.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertNotNull($receivedMeta);
        $this->assertArrayHasKey('id', $receivedMeta);
        $this->assertArrayHasKey('stream', $receivedMeta);
        $this->assertSame('meta.handler.stream', $receivedMeta['stream']);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('meta.handler.stream');
    }

    public function testDeserializesInvalidJsonAsString(): void
    {
        // Publish non-JSON string
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('plain.invalid.stream', 'plain text message');

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer, autoDeserialize: true);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('plain.invalid.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsString($receivedMessage);
        $this->assertSame('plain text message', $receivedMessage);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('plain.invalid.stream');
    }


    public function testPublishAndVerifyStreamExists(): void
    {
        // Simple test that doesn't require consumer loop
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('verify.stream', json_encode(['test' => 'data']));

        $this->connection->connect();
        $redis = $this->connection->getClient();

        $messages = $redis->xRange('verify.stream', '-', '+');
        $this->assertNotEmpty($messages);

        $firstMessage = reset($messages);
        $decoded = json_decode($firstMessage['message'], true);
        $this->assertSame('data', $decoded['test']);

        // Cleanup
        $redis->del('verify.stream');
    }

    public function testFluentApiWithRedis(): void
    {
        $consumer = (new MessageConsumer())
            ->setRedis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testFluentApiMultipleLayers(): void
    {
        $consumer = (new MessageConsumer())
            ->setRedis($_ENV['REDIS_HOST'] ?? 'redis', 6379, null, [], 0);

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testThrowsExceptionWhenNoConsumersConfigured(): void
    {
        $consumer = new MessageConsumer();
        $handler = new CallbackHandler(fn($m, $meta) => false);

        $this->expectException(\JardisPsr\Messaging\Exception\ConsumerException::class);
        $this->expectExceptionMessage('No consumers configured');

        $consumer->consume('test', $handler);
    }

    public function testAutoDeserializeCanBeDisabled(): void
    {
        $consumer = (new MessageConsumer())
            ->setRedis($_ENV['REDIS_HOST'] ?? 'redis', 6379)
            ->autoDeserialize(false);

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testStopAllConsumers(): void
    {
        $consumer = (new MessageConsumer())
            ->setRedis($_ENV['REDIS_HOST'] ?? 'redis', 6379);

        $consumer->stop();

        $this->assertTrue(true);
    }

    public function testDeserializeEmptyString(): void
    {
        // Test that empty string is handled correctly
        $publisher = new RedisPublisher($this->connection, useStreams: true);
        $publisher->publish('empty.stream', '');

        $redisConsumer = new RedisConsumer($this->connection, useStreams: true);
        $messageConsumer = new MessageConsumer($redisConsumer, autoDeserialize: true);

        $receivedMessage = null;
        $handler = new CallbackHandler(function (string|array $msg, array $meta) use (&$receivedMessage): bool {
            $receivedMessage = $msg;
            return false;
        });

        $messageConsumer->consume('empty.stream', $handler, ['start_id' => '0', 'block' => 100]);

        $this->assertIsString($receivedMessage);
        $this->assertSame('', $receivedMessage);

        // Cleanup
        $this->connection->connect();
        $this->connection->getClient()->del('empty.stream');
    }

}
