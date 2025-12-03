<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Unit;

use JardisPsr\Messaging\ConsumerInterface;
use JardisPsr\Messaging\MessageHandlerInterface;
use JardisPsr\Messaging\Exception\ConsumerException;
use JardisCore\Messaging\MessageConsumer;
use PHPUnit\Framework\TestCase;

class MessageConsumerTest extends TestCase
{
    public function testConstructorWithLegacyConsumer(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $consumer = new MessageConsumer($mockConsumer);

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testConstructorWithoutConsumer(): void
    {
        $consumer = new MessageConsumer();

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testConstructorWithAutoDeserializeDisabled(): void
    {
        $consumer = new MessageConsumer(null, false);

        $this->assertInstanceOf(MessageConsumer::class, $consumer);
    }

    public function testAutoDeserializeToggle(): void
    {
        $consumer = new MessageConsumer();

        $result = $consumer->autoDeserialize(false);

        $this->assertSame($consumer, $result); // Fluent interface
    }

    public function testThrowsExceptionWhenNoConsumersConfigured(): void
    {
        $consumer = new MessageConsumer();
        $handler = $this->createMock(MessageHandlerInterface::class);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('No consumers configured');

        $consumer->consume('test-topic', $handler);
    }

    public function testStopWithNoConsumers(): void
    {
        $consumer = new MessageConsumer();
        $consumer->stop();

        $this->assertTrue(true); // No exception thrown
    }

    public function testStopWithLegacyConsumer(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockConsumer->expects($this->once())
            ->method('stop');

        $consumer = new MessageConsumer($mockConsumer);
        $consumer->stop();
    }

    public function testConsumeWithLegacyConsumerSuccess(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->with('test-topic', $this->isType('callable'), []);

        $consumer = new MessageConsumer($mockConsumer);
        $consumer->consume('test-topic', $mockHandler);
    }

    public function testConsumeWithLegacyConsumerFailure(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willThrowException(new \Exception('Connection failed'));

        $consumer = new MessageConsumer($mockConsumer);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('All consumer layers failed');

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testConsumeCallbackWithAutoDeserializeEnabled(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true); // Auto-deserialize enabled
        $consumer->consume('test-topic', $mockHandler);

        // Test the callback with JSON
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with(['key' => 'value'], ['meta' => 'data'])
            ->willReturn(true);

        $result = $capturedCallback('{"key":"value"}', ['meta' => 'data']);
        $this->assertTrue($result);
    }

    public function testConsumeCallbackWithAutoDeserializeDisabled(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, false); // Auto-deserialize disabled
        $consumer->consume('test-topic', $mockHandler);

        // Test the callback with raw string
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('{"key":"value"}', ['meta' => 'data'])
            ->willReturn(true);

        $result = $capturedCallback('{"key":"value"}', ['meta' => 'data']);
        $this->assertTrue($result);
    }

    public function testDeserializeValidJson(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true);
        $consumer->consume('test-topic', $mockHandler);

        // Test deserialize with valid JSON
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with(['name' => 'John', 'age' => 30], [])
            ->willReturn(true);

        $capturedCallback('{"name":"John","age":30}', []);
    }

    public function testDeserializeInvalidJson(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true);
        $consumer->consume('test-topic', $mockHandler);

        // Test deserialize with invalid JSON (should return raw string)
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('not-json', [])
            ->willReturn(true);

        $capturedCallback('not-json', []);
    }

    public function testDeserializeEmptyString(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true);
        $consumer->consume('test-topic', $mockHandler);

        // Test deserialize with empty string (should return empty string)
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('', [])
            ->willReturn(true);

        $capturedCallback('', []);
    }

    public function testDeserializeJsonNumber(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true);
        $consumer->consume('test-topic', $mockHandler);

        // Test deserialize with JSON number (not array, should return raw)
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('123', [])
            ->willReturn(true);

        $capturedCallback('123', []);
    }

    public function testDeserializeJsonString(): void
    {
        $mockConsumer = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        $capturedCallback = null;

        $mockConsumer->expects($this->once())
            ->method('consume')
            ->willReturnCallback(function ($topic, $callback, $options) use (&$capturedCallback) {
                $capturedCallback = $callback;
            });

        $consumer = new MessageConsumer($mockConsumer, true);
        $consumer->consume('test-topic', $mockHandler);

        // Test deserialize with JSON string (not array, should return raw)
        $mockHandler->expects($this->once())
            ->method('handle')
            ->with('"hello"', [])
            ->willReturn(true);

        $capturedCallback('"hello"', []);
    }

    public function testMultipleConsumersFallback(): void
    {
        $mockConsumer1 = $this->createMock(ConsumerInterface::class);
        $mockConsumer2 = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        // First consumer fails
        $mockConsumer1->expects($this->once())
            ->method('consume')
            ->willThrowException(new \Exception('Consumer 1 failed'));

        // Second consumer succeeds
        $mockConsumer2->expects($this->once())
            ->method('consume');

        $consumer = new MessageConsumer($mockConsumer1);

        // Inject second consumer via reflection (testing fallback logic)
        $reflection = new \ReflectionClass($consumer);
        $consumersProperty = $reflection->getProperty('consumers');
        $consumersProperty->setAccessible(true);
        $consumers = $consumersProperty->getValue($consumer);
        $consumers[] = [
            'type' => 'fallback',
            'consumer' => $mockConsumer2,
            'priority' => 1
        ];
        $consumersProperty->setValue($consumer, $consumers);

        $consumer->consume('test-topic', $mockHandler);
    }

    public function testAllConsumersFail(): void
    {
        $mockConsumer1 = $this->createMock(ConsumerInterface::class);
        $mockConsumer2 = $this->createMock(ConsumerInterface::class);
        $mockHandler = $this->createMock(MessageHandlerInterface::class);

        // Both consumers fail
        $mockConsumer1->expects($this->once())
            ->method('consume')
            ->willThrowException(new \Exception('Consumer 1 failed'));

        $mockConsumer2->expects($this->once())
            ->method('consume')
            ->willThrowException(new \Exception('Consumer 2 failed'));

        $consumer = new MessageConsumer($mockConsumer1);

        // Inject second consumer
        $reflection = new \ReflectionClass($consumer);
        $consumersProperty = $reflection->getProperty('consumers');
        $consumersProperty->setAccessible(true);
        $consumers = $consumersProperty->getValue($consumer);
        $consumers[] = [
            'type' => 'fallback',
            'consumer' => $mockConsumer2,
            'priority' => 1
        ];
        $consumersProperty->setValue($consumer, $consumers);

        $this->expectException(ConsumerException::class);
        $this->expectExceptionMessage('All consumer layers failed');

        $consumer->consume('test-topic', $mockHandler);
    }
}
