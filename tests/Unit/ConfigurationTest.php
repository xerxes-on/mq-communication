<?php

declare(strict_types=1);

it('loads default configuration values', function (): void {
    expect(config('rabbitmq.host'))->toBe('127.0.0.1')
        ->and(config('rabbitmq.port'))->toBe(5672)
        ->and(config('rabbitmq.user'))->toBe('guest')
        ->and(config('rabbitmq.vhost'))->toBe('/');
});

it('has outbox configuration', function (): void {
    expect(config('rabbitmq.outbox.enabled'))->toBeTrue()
        ->and(config('rabbitmq.outbox.connection'))->toBe('outbox')
        ->and(config('rabbitmq.outbox.table'))->toBe('outbox_messages');
});

it('has dead letter queue configuration', function (): void {
    expect(config('rabbitmq.dead_letter.enabled'))->toBeTrue()
        ->and(config('rabbitmq.dead_letter.max_retries'))->toBe(3)
        ->and(config('rabbitmq.dead_letter.exchange'))->toBe('dlx.failed')
        ->and(config('rabbitmq.dead_letter.retry_delays'))->toBeArray();
});

it('has consumer configuration', function (): void {
    expect(config('rabbitmq.consumer_prefetch'))->toBe(1)
        ->and(config('rabbitmq.event-consumer-mode'))->toBe('sync');
});

it('has ssl configuration', function (): void {
    expect(config('rabbitmq.ssl.enabled'))->toBeFalse()
        ->and(config('rabbitmq.ssl.verify_peer'))->toBeTrue();
});
