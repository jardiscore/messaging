<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Tests\Unit\Config;

use JardisCore\Messaging\Config\PublishOptions;
use PHPUnit\Framework\TestCase;

class PublishOptionsTest extends TestCase
{
    public function testCreatesWithDefaults(): void
    {
        $options = new PublishOptions();

        $this->assertNull($options->priority);
        $this->assertNull($options->ttl);
        $this->assertSame([], $options->metadata);
    }

    public function testCreatesWithAllValues(): void
    {
        $options = new PublishOptions(
            priority: 5,
            ttl: 60000,
            metadata: ['key' => 'value']
        );

        $this->assertSame(5, $options->priority);
        $this->assertSame(60000, $options->ttl);
        $this->assertSame(['key' => 'value'], $options->metadata);
    }

    public function testAcceptsMinimumPriority(): void
    {
        $options = new PublishOptions(priority: 0);

        $this->assertSame(0, $options->priority);
    }

    public function testAcceptsMaximumPriority(): void
    {
        $options = new PublishOptions(priority: 9);

        $this->assertSame(9, $options->priority);
    }

    public function testThrowsExceptionForNegativePriority(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Priority must be between 0 and 9, got -1');

        new PublishOptions(priority: -1);
    }

    public function testThrowsExceptionForTooHighPriority(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Priority must be between 0 and 9, got 10');

        new PublishOptions(priority: 10);
    }

    public function testThrowsExceptionForNegativeTtl(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('TTL must be positive, got -1');

        new PublishOptions(ttl: -1);
    }

    public function testAcceptsZeroTtl(): void
    {
        $options = new PublishOptions(ttl: 0);

        $this->assertSame(0, $options->ttl);
    }

    public function testFromArrayWithAllValues(): void
    {
        $array = [
            'priority' => 7,
            'ttl' => 30000,
            'metadata' => ['trace_id' => '123']
        ];

        $options = PublishOptions::fromArray($array);

        $this->assertSame(7, $options->priority);
        $this->assertSame(30000, $options->ttl);
        $this->assertSame(['trace_id' => '123'], $options->metadata);
    }

    public function testFromArrayWithHeadersAlias(): void
    {
        $array = [
            'headers' => ['x-custom' => 'value']
        ];

        $options = PublishOptions::fromArray($array);

        $this->assertSame(['x-custom' => 'value'], $options->metadata);
    }

    public function testFromArrayWithEmptyArray(): void
    {
        $options = PublishOptions::fromArray([]);

        $this->assertNull($options->priority);
        $this->assertNull($options->ttl);
        $this->assertSame([], $options->metadata);
    }

    public function testToArrayWithAllValues(): void
    {
        $options = new PublishOptions(
            priority: 5,
            ttl: 60000,
            metadata: ['key' => 'value']
        );

        $array = $options->toArray();

        $this->assertSame([
            'priority' => 5,
            'ttl' => 60000,
            'metadata' => ['key' => 'value']
        ], $array);
    }

    public function testToArrayWithDefaults(): void
    {
        $options = new PublishOptions();

        $array = $options->toArray();

        $this->assertSame([], $array);
    }

    public function testToArrayOmitsNullValues(): void
    {
        $options = new PublishOptions(priority: 5);

        $array = $options->toArray();

        $this->assertSame(['priority' => 5], $array);
        $this->assertArrayNotHasKey('ttl', $array);
        $this->assertArrayNotHasKey('metadata', $array);
    }

    public function testIsReadonly(): void
    {
        $options = new PublishOptions();

        $this->expectException(\Error::class);

        $options->priority = 5;
    }
}
