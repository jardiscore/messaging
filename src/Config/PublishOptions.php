<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Config;

/**
 * Value object for message publishing options
 *
 * Provides type-safe configuration for message publishing
 * with validation of values
 */
readonly class PublishOptions
{
    /**
     * @param int|null $priority Message priority (0-9, where 9 is highest)
     * @param int|null $ttl Time-to-live in milliseconds
     * @param array<string, mixed> $metadata Additional metadata/headers
     * @throws \InvalidArgumentException If values are invalid
     */
    public function __construct(
        public ?int $priority = null,
        public ?int $ttl = null,
        public array $metadata = [],
    ) {
        if ($this->priority !== null && ($this->priority < 0 || $this->priority > 9)) {
            throw new \InvalidArgumentException(
                "Priority must be between 0 and 9, got {$this->priority}"
            );
        }

        if ($this->ttl !== null && $this->ttl < 0) {
            throw new \InvalidArgumentException(
                "TTL must be positive, got {$this->ttl}"
            );
        }
    }

    /**
     * Create from array
     *
     * @param array<string, mixed> $options
     * @return self
     */
    public static function fromArray(array $options): self
    {
        return new self(
            priority: isset($options['priority']) ? (int) $options['priority'] : null,
            ttl: isset($options['ttl']) ? (int) $options['ttl'] : null,
            metadata: $options['metadata'] ?? $options['headers'] ?? [],
        );
    }

    /**
     * Convert to array
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $array = [];

        if ($this->priority !== null) {
            $array['priority'] = $this->priority;
        }

        if ($this->ttl !== null) {
            $array['ttl'] = $this->ttl;
        }

        if ($this->metadata !== []) {
            $array['metadata'] = $this->metadata;
        }

        return $array;
    }
}
