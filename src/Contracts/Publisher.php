<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Contracts;

interface Publisher
{
    /**
     * Publish a message to RabbitMQ.
     *
     * @param  array<string, mixed>|string  $payload
     * @param  array<string, mixed>  $options
     */
    public function publish(string $exchange, string $routingKey, array|string $payload, array $options = []): void;
}
