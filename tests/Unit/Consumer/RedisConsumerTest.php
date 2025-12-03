<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Unit\Consumer;

use JardisCore\Messaging\Connection\RedisConnection;
use JardisCore\Messaging\Consumer\RedisConsumer;
use PHPUnit\Framework\TestCase;
use Redis;

class RedisConsumerTest extends TestCase
{
    public function testConstructorWithPubSub(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: false);

        $this->assertInstanceOf(RedisConsumer::class, $consumer);
    }

    public function testConstructorWithStreams(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: true);

        $this->assertInstanceOf(RedisConsumer::class, $consumer);
    }

    public function testStop(): void
    {
        $connection = $this->createMock(RedisConnection::class);
        $consumer = new RedisConsumer($connection, useStreams: true);

        $consumer->stop();

        $this->assertTrue(true); // No exception
    }

    public function testStreamMetadataExtraction(): void
    {
        $this->markTestSkipped(
            'RedisConsumer with real consume() runs in loop. ' .
            'This test would require stopping the loop which is difficult with mocks. ' .
            'Integration tests cover this functionality.'
        );
    }

    public function testStreamMetadataWithMissingMessageField(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }

    public function testStreamWithEmptyXRead(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }

    public function testStreamWithCallbackException(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }

    public function testStreamOptionsExtraction(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }

    public function testStreamDefaultOptions(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }

    public function testAutoConnect(): void
    {
        $this->markTestSkipped('Requires stopping consume loop - covered by integration tests');
    }
}
