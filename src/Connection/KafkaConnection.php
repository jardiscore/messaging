<?php

declare(strict_types=1);

namespace JardisCore\Messaging\Connection;

use JardisCore\Messaging\Config\ConnectionConfig;
use JardisPsr\Messaging\ConnectionInterface;
use JardisPsr\Messaging\Exception\ConnectionException;
use RdKafka\Conf;
use RdKafka\Producer;

/**
 * Kafka connection manager
 *
 * Handles connection lifecycle to Kafka broker
 */
class KafkaConnection implements ConnectionInterface
{
    private ?Producer $producer = null;
    private bool $connected = false;

    public function __construct(
        private readonly ConnectionConfig $config
    ) {
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Establish connection to Kafka
     *
     * @throws ConnectionException
     */
    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        try {
            $conf = new Conf();
            $conf->set('metadata.broker.list', "{$this->config->host}:{$this->config->port}");

            // Set SASL authentication if credentials provided
            if ($this->config->username !== null && $this->config->password !== null) {
                $conf->set('security.protocol', 'SASL_SSL');
                $conf->set('sasl.mechanism', 'PLAIN');
                $conf->set('sasl.username', $this->config->username);
                $conf->set('sasl.password', $this->config->password);
            }

            // Apply additional Kafka-specific options
            foreach ($this->config->options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $conf->set($key, (string) $value);
                }
            }

            $this->producer = new Producer($conf);
            $this->connected = true;
        } catch (\Exception $e) {
            throw new ConnectionException(
                "Failed to create Kafka producer: {$e->getMessage()}",
                previous: $e
            );
        }
    }

    /**
     * Close connection to Kafka
     */
    public function disconnect(): void
    {
        if ($this->producer !== null && $this->connected) {
            $this->producer->flush(10000);
            $this->producer = null;
            $this->connected = false;
        }
    }

    /**
     * Check if connected
     */
    public function isConnected(): bool
    {
        return $this->connected && $this->producer !== null;
    }

    /**
     * Get the Kafka producer instance
     *
     * @throws ConnectionException if not connected
     */
    public function getClient(): Producer
    {
        if (!$this->isConnected() || $this->producer === null) {
            throw new ConnectionException('Not connected to Kafka');
        }

        return $this->producer;
    }
}
