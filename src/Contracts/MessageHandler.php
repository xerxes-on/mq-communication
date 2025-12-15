<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Contracts;

interface MessageHandler
{
    /**
     * Handle a consumed message.
     *
     * @param  array<string, mixed>  $payload
     */
    public function handle(array $payload, ?string $routingKey = null): void;
}
