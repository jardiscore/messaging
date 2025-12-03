<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Unit;

use JardisPsr\Messaging\PublisherInterface;
use JardisPsr\Messaging\Exception\PublishException;
use JardisCore\Messaging\MessagePublisher;
use JardisCore\Messaging\Validation\MessageValidator;
use PHPUnit\Framework\TestCase;

class MessagePublisherTest extends TestCase
{
    public function testConstructorWithLegacyPublisher(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);
        $publisher = new MessagePublisher($mockPublisher);

        $this->assertInstanceOf(MessagePublisher::class, $publisher);
    }

    public function testConstructorWithoutPublisher(): void
    {
        $publisher = new MessagePublisher();

        $this->assertInstanceOf(MessagePublisher::class, $publisher);
    }

    public function testConstructorWithCustomValidator(): void
    {
        $mockValidator = $this->createMock(MessageValidator::class);
        $publisher = new MessagePublisher(null, $mockValidator);

        $this->assertInstanceOf(MessagePublisher::class, $publisher);
    }

    public function testThrowsExceptionWhenNoPublishersConfigured(): void
    {
        $publisher = new MessagePublisher();

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('No publishers configured');

        $publisher->publish('test-topic', 'message');
    }

    public function testThrowsExceptionWhenNoPublishersConfiguredForPublishToAll(): void
    {
        $publisher = new MessagePublisher();

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('No publishers configured');

        $publisher->publishToAll('test-topic', 'message');
    }

    public function testPublishWithLegacyPublisherSuccess(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', 'message');

        $this->assertTrue($result);
    }

    public function testPublishWithLegacyPublisherFailure(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willThrowException(new \Exception('Connection failed'));

        $publisher = new MessagePublisher($mockPublisher);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('All publisher layers failed');

        $publisher->publish('test-topic', 'message');
    }

    public function testPublishStringMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'Hello World', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', 'Hello World');

        $this->assertTrue($result);
    }

    public function testPublishArrayMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', '{"key":"value"}', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', ['key' => 'value']);

        $this->assertTrue($result);
    }

    public function testPublishObjectMessage(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $object = new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['type' => 'test', 'value' => 123];
            }
        };

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', '{"type":"test","value":123}', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', $object);

        $this->assertTrue($result);
    }

    public function testPublishToAllWithMultiplePublishers(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', [])
            ->willReturn(true);

        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', [])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher1);

        // Inject second publisher via reflection
        $reflection = new \ReflectionClass($publisher);
        $publishersProperty = $reflection->getProperty('publishers');
        $publishersProperty->setAccessible(true);
        $publishers = $publishersProperty->getValue($publisher);
        $publishers[] = [
            'type' => 'secondary',
            'publisher' => $mockPublisher2,
            'priority' => 1
        ];
        $publishersProperty->setValue($publisher, $publishers);

        $results = $publisher->publishToAll('test-topic', 'message');

        $this->assertIsArray($results);
        $this->assertCount(2, $results);
        $this->assertTrue($results['legacy']);
        $this->assertTrue($results['secondary']);
    }

    public function testPublishToAllWithPartialFailure(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->willThrowException(new \Exception('Publisher 1 failed'));

        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher1);

        // Inject second publisher
        $reflection = new \ReflectionClass($publisher);
        $publishersProperty = $reflection->getProperty('publishers');
        $publishersProperty->setAccessible(true);
        $publishers = $publishersProperty->getValue($publisher);
        $publishers[] = [
            'type' => 'secondary',
            'publisher' => $mockPublisher2,
            'priority' => 1
        ];
        $publishersProperty->setValue($publisher, $publishers);

        $results = $publisher->publishToAll('test-topic', 'message');

        $this->assertIsArray($results);
        $this->assertFalse($results['legacy']);
        $this->assertTrue($results['secondary']);
    }

    public function testMultiplePublishersFallback(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        // First publisher fails
        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->willThrowException(new \Exception('Publisher 1 failed'));

        // Second publisher succeeds
        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher1);

        // Inject second publisher
        $reflection = new \ReflectionClass($publisher);
        $publishersProperty = $reflection->getProperty('publishers');
        $publishersProperty->setAccessible(true);
        $publishers = $publishersProperty->getValue($publisher);
        $publishers[] = [
            'type' => 'fallback',
            'publisher' => $mockPublisher2,
            'priority' => 1
        ];
        $publishersProperty->setValue($publisher, $publishers);

        $result = $publisher->publish('test-topic', 'message');
        $this->assertTrue($result);
    }

    public function testAllPublishersFail(): void
    {
        $mockPublisher1 = $this->createMock(PublisherInterface::class);
        $mockPublisher2 = $this->createMock(PublisherInterface::class);

        // Both publishers fail
        $mockPublisher1->expects($this->once())
            ->method('publish')
            ->willThrowException(new \Exception('Publisher 1 failed'));

        $mockPublisher2->expects($this->once())
            ->method('publish')
            ->willThrowException(new \Exception('Publisher 2 failed'));

        $publisher = new MessagePublisher($mockPublisher1);

        // Inject second publisher
        $reflection = new \ReflectionClass($publisher);
        $publishersProperty = $reflection->getProperty('publishers');
        $publishersProperty->setAccessible(true);
        $publishers = $publishersProperty->getValue($publisher);
        $publishers[] = [
            'type' => 'fallback',
            'publisher' => $mockPublisher2,
            'priority' => 1
        ];
        $publishersProperty->setValue($publisher, $publishers);

        $this->expectException(PublishException::class);
        $this->expectExceptionMessage('All publisher layers failed');

        $publisher->publish('test-topic', 'message');
    }

    public function testPublishWithCustomOptions(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with('test-topic', 'message', ['custom' => 'option'])
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', 'message', ['custom' => 'option']);

        $this->assertTrue($result);
    }

    public function testSerializeComplexArray(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);

        $complexArray = [
            'user' => [
                'id' => 123,
                'name' => 'John',
                'meta' => [
                    'roles' => ['admin', 'user']
                ]
            ],
            'timestamp' => 1234567890,
            'nullable' => null
        ];

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->with(
                'test-topic',
                $this->isType('string'),
                []
            )
            ->willReturnCallback(function ($topic, $message, $options) use ($complexArray) {
                $decoded = json_decode($message, true);
                $this->assertEquals($complexArray, $decoded);
                return true;
            });

        $publisher = new MessagePublisher($mockPublisher);
        $result = $publisher->publish('test-topic', $complexArray);

        $this->assertTrue($result);
    }

    public function testValidatorIsCalledForArrays(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);
        $mockValidator = $this->createMock(MessageValidator::class);

        $mockValidator->expects($this->once())
            ->method('validate')
            ->with(['key' => 'value']);

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher, $mockValidator);
        $publisher->publish('test-topic', ['key' => 'value']);
    }

    public function testValidatorNotCalledForStrings(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);
        $mockValidator = $this->createMock(MessageValidator::class);

        $mockValidator->expects($this->never())
            ->method('validate');

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher, $mockValidator);
        $publisher->publish('test-topic', 'string message');
    }

    public function testValidatorNotCalledForObjects(): void
    {
        $mockPublisher = $this->createMock(PublisherInterface::class);
        $mockValidator = $this->createMock(MessageValidator::class);

        $object = new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['test' => 'data'];
            }
        };

        $mockValidator->expects($this->never())
            ->method('validate');

        $mockPublisher->expects($this->once())
            ->method('publish')
            ->willReturn(true);

        $publisher = new MessagePublisher($mockPublisher, $mockValidator);
        $publisher->publish('test-topic', $object);
    }
}
