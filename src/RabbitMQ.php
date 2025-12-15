<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use Xerxes\RabbitMQ\Contracts\Publisher;

class RabbitMQ implements Publisher
{
    private ?AMQPStreamConnection $connection = null;

    private ?AMQPChannel $channel = null;

    public function __construct()
    {
        // Deferred connection - only connect when actually used
    }

    private function connect(): void
    {
        if ($this->connection !== null) {
            return;
        }

        $this->connection = new AMQPStreamConnection(
            host: config('rabbitmq.host'),
            port: config('rabbitmq.port'),
            user: config('rabbitmq.user'),
            password: config('rabbitmq.password'),
            vhost: config('rabbitmq.vhost'),
            insist: config('rabbitmq.insist', false),
            login_method: config('rabbitmq.login_method', 'AMQPLAIN'),
            login_response: config('rabbitmq.login_response'),
            locale: config('rabbitmq.locale', 'en_US'),
            connection_timeout: config('rabbitmq.connection_timeout', 3.0),
            read_write_timeout: config('rabbitmq.read_write_timeout', 3.0),
            context: config('rabbitmq.context'),
            keepalive: config('rabbitmq.keepalive', false),
            heartbeat: config('rabbitmq.heartbeat', 0),
            channel_rpc_timeout: config('rabbitmq.channel_rpc_timeout', 0.0),
        );

        $this->channel = $this->connection->channel();
    }

    public function queue(): RabbitMQQueue
    {
        $this->connect();

        return new RabbitMQQueue($this->channel);
    }

    public function exchange(): RabbitMQExchange
    {
        $this->connect();

        return new RabbitMQExchange($this->connection);
    }

    public function message(): RabbitMQMessage
    {
        $this->connect();

        return new RabbitMQMessage($this->channel);
    }

    public function consume(): RabbitMQConsumer
    {
        $this->connect();

        return new RabbitMQConsumer($this->channel);
    }

    /**
     * Get the AMQP channel directly.
     */
    public function channel(bool $fresh = false): AMQPChannel
    {
        $this->connect();

        if ($fresh) {
            return $this->connection->channel();
        }

        return $this->channel;
    }

    /**
     * Backward compatible publish method.
     *
     * @param  array<string, mixed>|string  $payload
     * @param  array<string, mixed>  $options
     */
    public function publish(string $exchange, string $routingKey, array|string $payload, array $options = []): void
    {
        $this->connect();

        $messageBuilder = $this->message()
            ->persistent()
            ->viaExchange($exchange)
            ->route($routingKey)
            ->withPayload(is_array($payload) ? $payload : ['data' => $payload]);

        $messageBuilder->publish();
    }

    /**
     * Declare a queue with exchange bindings.
     *
     * @param  array<string>  $bindings  Routing keys to bind
     * @param  array<string, mixed>  $queueOptions
     * @param  array<string, mixed>  $exchangeOptions
     * @return array{string, int, int}
     */
    public function declareQueue(
        string $queueName,
        string $exchange,
        array $bindings = [],
        array $queueOptions = [],
        array $exchangeOptions = []
    ): array {
        $this->connect();

        $exchangeType = $exchangeOptions['type'] ?? 'topic';
        $exchangeDurable = $exchangeOptions['durable'] ?? true;
        $exchangeAutoDelete = $exchangeOptions['auto_delete'] ?? false;

        $this->channel->exchange_declare(
            $exchange,
            $exchangeType,
            false,
            $exchangeDurable,
            $exchangeAutoDelete
        );

        $queueDurable = $queueOptions['durable'] ?? true;
        $queueAutoDelete = $queueOptions['auto_delete'] ?? false;
        $queueExclusive = $queueOptions['exclusive'] ?? false;

        $result = $this->channel->queue_declare(
            $queueName,
            false,
            $queueDurable,
            $queueExclusive,
            $queueAutoDelete
        );

        foreach ($bindings as $routingKey) {
            $this->channel->queue_bind($queueName, $exchange, $routingKey);
        }

        return $result;
    }

    /**
     * Wait for messages on the channel.
     */
    public function wait(?float $timeout = null, bool $nonBlocking = false): void
    {
        $this->connect();

        if ($nonBlocking) {
            $this->channel->wait(null, true, $timeout);
        } else {
            $this->channel->wait(null, false, $timeout);
        }
    }

    public function __destruct()
    {
        if ($this->connection !== null) {
            $this->connection->close();
        }
        if ($this->channel !== null) {
            $this->channel->close();
        }
    }
}
