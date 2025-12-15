<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Services;

use Illuminate\Support\Collection;
use Xerxes\RabbitMQ\Enums\OutboxStatus;
use Xerxes\RabbitMQ\Models\OutboxMessage;

class OutboxService
{
    /**
     * @param  array<string, mixed>  $payload
     */
    public function store(string $exchange, ?string $routingKey, array $payload): OutboxMessage
    {
        return OutboxMessage::create([
            'exchange' => $exchange,
            'routing_key' => $routingKey,
            'payload' => $payload,
            'status' => OutboxStatus::Pending,
        ]);
    }

    /**
     * @return Collection<int, OutboxMessage>
     */
    public function getPendingMessages(int $limit = 100): Collection
    {
        $query = OutboxMessage::pending()
            ->orderBy('created_at', 'asc');

        $allowedExchanges = config('rabbitmq.outbox.exchanges', []);
        if (! empty($allowedExchanges)) {
            $query->whereIn('exchange', $allowedExchanges);
        }

        return $query->limit($limit)->get();
    }

    public function markAsPublished(OutboxMessage $message): void
    {
        $message->markAsPublished();
    }

    public function markAsFailed(OutboxMessage $message, string $errorMessage): void
    {
        $message->markAsFailed($errorMessage);
    }

    public function getConnection(): string
    {
        return config('rabbitmq.outbox.connection', 'outbox');
    }

    public function retryMessage(OutboxMessage $message): void
    {
        $message->update([
            'status' => OutboxStatus::Pending,
            'error_message' => null,
        ]);
    }
}
