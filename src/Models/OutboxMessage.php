<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Models;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use Xerxes\RabbitMQ\Enums\OutboxStatus;

class OutboxMessage extends Model
{
    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);

        $this->connection = config('rabbitmq.outbox.connection', 'outbox');
    }

    public function getTable(): string
    {
        return config('rabbitmq.outbox.table', 'outbox_messages');
    }

    /** @var list<string> */
    protected $fillable = [
        'exchange',
        'routing_key',
        'payload',
        'status',
        'published_at',
        'retry_count',
        'error_message',
    ];

    /**
     * @return array<string, string>
     */
    protected function casts(): array
    {
        return [
            'payload' => 'array',
            'status' => OutboxStatus::class,
            'published_at' => 'datetime',
        ];
    }

    /**
     * @param  Builder<OutboxMessage>  $query
     * @return Builder<OutboxMessage>
     */
    public function scopePending(Builder $query): Builder
    {
        return $query->where('status', OutboxStatus::Pending);
    }

    /**
     * @param  Builder<OutboxMessage>  $query
     * @return Builder<OutboxMessage>
     */
    public function scopeFailed(Builder $query): Builder
    {
        return $query->where('status', OutboxStatus::Failed);
    }

    public function markAsPublished(): void
    {
        $this->delete();
    }

    public function markAsFailed(string $errorMessage): void
    {
        $this->update([
            'status' => OutboxStatus::Failed,
            'error_message' => $errorMessage,
            'retry_count' => $this->retry_count + 1,
        ]);
    }
}
