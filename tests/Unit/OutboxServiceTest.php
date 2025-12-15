<?php

declare(strict_types=1);

use Xerxes\RabbitMQ\Enums\OutboxStatus;
use Xerxes\RabbitMQ\Models\OutboxMessage;
use Xerxes\RabbitMQ\Services\OutboxService;

beforeEach(function (): void {
    $this->outboxService = new OutboxService;
});

it('stores a message in the outbox', function (): void {
    $message = $this->outboxService->store(
        'test.exchange',
        'test.routing.key',
        ['event' => 'test', 'data' => ['foo' => 'bar']]
    );

    expect($message)->toBeInstanceOf(OutboxMessage::class)
        ->and($message->exchange)->toBe('test.exchange')
        ->and($message->routing_key)->toBe('test.routing.key')
        ->and($message->payload)->toBe(['event' => 'test', 'data' => ['foo' => 'bar']])
        ->and($message->status)->toBe(OutboxStatus::Pending);
});

it('retrieves pending messages', function (): void {
    $this->outboxService->store('exchange1', 'key1', ['data' => 1]);
    $this->outboxService->store('exchange2', 'key2', ['data' => 2]);

    $pending = $this->outboxService->getPendingMessages();

    expect($pending)->toHaveCount(2);
});

it('limits retrieved pending messages', function (): void {
    $this->outboxService->store('exchange1', 'key1', ['data' => 1]);
    $this->outboxService->store('exchange2', 'key2', ['data' => 2]);
    $this->outboxService->store('exchange3', 'key3', ['data' => 3]);

    $pending = $this->outboxService->getPendingMessages(2);

    expect($pending)->toHaveCount(2);
});

it('marks a message as published by deleting it', function (): void {
    $message = $this->outboxService->store('exchange', 'key', ['data' => 'test']);

    $this->outboxService->markAsPublished($message);

    expect(OutboxMessage::find($message->id))->toBeNull();
});

it('marks a message as failed', function (): void {
    $message = $this->outboxService->store('exchange', 'key', ['data' => 'test']);

    $this->outboxService->markAsFailed($message, 'Connection refused');

    $message->refresh();

    expect($message->status)->toBe(OutboxStatus::Failed)
        ->and($message->error_message)->toBe('Connection refused')
        ->and($message->retry_count)->toBe(1);
});

it('retries a failed message', function (): void {
    $message = $this->outboxService->store('exchange', 'key', ['data' => 'test']);
    $this->outboxService->markAsFailed($message, 'Error');

    $this->outboxService->retryMessage($message);

    $message->refresh();

    expect($message->status)->toBe(OutboxStatus::Pending)
        ->and($message->error_message)->toBeNull();
});

it('returns the configured connection', function (): void {
    expect($this->outboxService->getConnection())->toBe('outbox');
});
