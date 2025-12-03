<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Unit\Consumer;

use JardisCore\Messaging\Config\ConnectionConfig;
use JardisCore\Messaging\Consumer\KafkaConsumer;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;

class KafkaConsumerTest extends TestCase
{
    public function testConstructor(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testConstructorWithCustomConfig(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $customConfig = ['auto.offset.reset' => 'latest'];

        $consumer = new KafkaConsumer($config, 'test-group', $customConfig);

        $this->assertInstanceOf(KafkaConsumer::class, $consumer);
    }

    public function testStop(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $consumer->stop();

        $this->assertTrue(true); // No exception
    }

    public function testHandleMessageWithNoError(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test payload';
        $mockMessage->partition = 0;
        $mockMessage->offset = 123;
        $mockMessage->timestamp = 1234567890;
        $mockMessage->key = 'test-key';
        $mockMessage->topic_name = 'test-topic';

        $callbackInvoked = false;
        $receivedPayload = null;
        $receivedMetadata = null;

        $callback = function ($payload, $metadata) use (&$callbackInvoked, &$receivedPayload, &$receivedMetadata) {
            $callbackInvoked = true;
            $receivedPayload = $payload;
            $receivedMetadata = $metadata;
            return true;
        };

        // Use reflection to access private handleMessage method
        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertTrue($result);
        $this->assertTrue($callbackInvoked);
        $this->assertEquals('test payload', $receivedPayload);
        $this->assertArrayHasKey('partition', $receivedMetadata);
        $this->assertEquals(0, $receivedMetadata['partition']);
        $this->assertEquals(123, $receivedMetadata['offset']);
        $this->assertEquals('test-topic', $receivedMetadata['topic']);
        $this->assertEquals('kafka', $receivedMetadata['type']);
    }

    public function testHandleMessagePartitionEof(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;

        $callback = function () {
            $this->fail('Callback should not be invoked for EOF');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertFalse($result); // No message processed
    }

    public function testHandleMessageTimeout(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        $callback = function () {
            $this->fail('Callback should not be invoked for timeout');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertFalse($result); // No message processed
    }

    public function testHandleMessageWithCallbackReturnFalse(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test';
        $mockMessage->partition = 0;
        $mockMessage->offset = 0;
        $mockMessage->timestamp = 0;
        $mockMessage->key = null;
        $mockMessage->topic_name = 'test';

        $callback = function () {
            return false; // Stop consuming
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertTrue($result); // Message was processed
    }

    public function testHandleMessageWithCallbackException(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $mockMessage->payload = 'test';
        $mockMessage->partition = 0;
        $mockMessage->offset = 0;
        $mockMessage->timestamp = 0;
        $mockMessage->key = null;
        $mockMessage->topic_name = 'test';

        $callback = function () {
            throw new \Exception('Callback error');
        };

        $reflection = new \ReflectionClass($consumer);
        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        // Should not throw, but log error
        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertTrue($result); // Message was processed despite error
    }

    public function testHandleMessageWithUnknownError(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        $mockMessage = $this->createMock(Message::class);
        $mockMessage->err = 999; // Unknown error code
        $mockMessage->method('errstr')->willReturn('Unknown error');

        $callback = function () {
            $this->fail('Callback should not be invoked for errors');
        };

        // Set running to true via reflection
        $reflection = new \ReflectionClass($consumer);
        $runningProperty = $reflection->getProperty('running');
        $runningProperty->setAccessible(true);
        $runningProperty->setValue($consumer, true);

        $method = $reflection->getMethod('handleMessage');
        $method->setAccessible(true);

        $result = $method->invoke($consumer, $mockMessage, $callback);

        $this->assertFalse($result); // No message processed
    }

    public function testDestruct(): void
    {
        $config = new ConnectionConfig(host: 'localhost', port: 9092);
        $consumer = new KafkaConsumer($config, 'test-group');

        unset($consumer);

        $this->assertTrue(true); // Destructor called without error
    }
}
