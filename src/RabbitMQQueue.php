<?php

namespace Xerxes\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

class RabbitMQQueue
{
    private AMQPChannel $channel;

    private bool $durable = false;

    private string $name;

    /**
     * @var array<string, mixed>
     */
    private array $arguments = [];

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    public function durable(): RabbitMQQueue
    {
        $this->durable = true;

        return $this;
    }

    public function name(string $name): RabbitMQQueue
    {
        $this->name = $name;

        return $this;
    }

    /**
     * Set queue arguments (e.g., x-dead-letter-exchange, x-message-ttl).
     *
     * @param  array<string, mixed>  $arguments
     */
    public function withArguments(array $arguments): RabbitMQQueue
    {
        $this->arguments = array_merge($this->arguments, $arguments);

        return $this;
    }

    public function declare(): RabbitMQQueue
    {
        $arguments = ! empty($this->arguments) ? new AMQPTable($this->arguments) : [];

        $this->channel->queue_declare(
            $this->name,
            false,
            $this->durable,
            false,
            false,
            false,
            $arguments
        );

        return $this;
    }

    public function bindTo(string $exchange, string $route = ''): void
    {
        $this->channel->queue_bind($this->name, $exchange, $route);
    }
}
