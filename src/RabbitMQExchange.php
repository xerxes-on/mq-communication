<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class RabbitMQExchange
{
    private AMQPChannel $channel;

    private string $name = '';

    private string $type = 'topic';

    private bool $durable = false;

    public function __construct(AMQPStreamConnection $connection)
    {
        $this->channel = $connection->channel();
    }

    public function name(string $name): self
    {
        $this->name = $name;

        return $this;
    }

    public function type(string $type): self
    {
        $this->type = $type;

        return $this;
    }

    public function durable(): self
    {
        $this->durable = true;

        return $this;
    }

    public function declare(): void
    {
        $this->channel->exchange_declare(
            $this->name,
            $this->type,
            false,
            $this->durable,
            false
        );
    }

    public function __destruct()
    {
        $this->channel->close();
    }
}
