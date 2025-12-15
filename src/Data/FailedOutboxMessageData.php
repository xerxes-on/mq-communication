<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Data;

use Illuminate\Support\CarbonImmutable;
use Xerxes\RabbitMQ\Models\OutboxMessage;

final class FailedOutboxMessageData
{
    public function __construct(
        public readonly int $outboxMessageId,
        public readonly string $exchange,
        public readonly ?string $routingKey,
        public readonly array $payload,
        public readonly string $errorMessage,
        public readonly CarbonImmutable $failedAt,
    ) {}

    public static function fromOutboxMessage(OutboxMessage $message, string $errorMessage): self
    {
        return new self(
            outboxMessageId: $message->id,
            exchange: $message->exchange,
            routingKey: $message->routing_key,
            payload: $message->payload,
            errorMessage: $errorMessage,
            failedAt: now()->toImmutable()
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'outbox_message_id' => $this->outboxMessageId,
            'exchange' => $this->exchange,
            'routing_key' => $this->routingKey,
            'payload' => $this->payload,
            'error_message' => $this->errorMessage,
            'failed_at' => $this->failedAt,
        ];
    }
}
