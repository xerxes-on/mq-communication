<?php

declare(strict_types=1);

use Xerxes\RabbitMQ\Enums\ConsumerMode;
use Xerxes\RabbitMQ\Enums\OutboxStatus;

describe('OutboxStatus', function (): void {
    it('has pending status', function (): void {
        expect(OutboxStatus::Pending->value)->toBe('pending');
    });

    it('has failed status', function (): void {
        expect(OutboxStatus::Failed->value)->toBe('failed');
    });

    it('can be created from value', function (): void {
        expect(OutboxStatus::from('pending'))->toBe(OutboxStatus::Pending);
        expect(OutboxStatus::from('failed'))->toBe(OutboxStatus::Failed);
    });
});

describe('ConsumerMode', function (): void {
    it('has sync mode', function (): void {
        expect(ConsumerMode::Sync->value)->toBe('sync');
    });

    it('has kind-sync mode', function (): void {
        expect(ConsumerMode::KindSync->value)->toBe('kind-sync');
    });

    it('has job mode', function (): void {
        expect(ConsumerMode::Job->value)->toBe('job');
    });

    it('can be created from value', function (): void {
        expect(ConsumerMode::from('sync'))->toBe(ConsumerMode::Sync);
        expect(ConsumerMode::from('kind-sync'))->toBe(ConsumerMode::KindSync);
        expect(ConsumerMode::from('job'))->toBe(ConsumerMode::Job);
    });
});
