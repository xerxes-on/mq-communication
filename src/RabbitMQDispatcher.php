<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ;

use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Log;
use Throwable;
use Xerxes\RabbitMQ\Support\ShouldPublish;

class RabbitMQDispatcher extends Dispatcher
{
    public function dispatch($event, $payload = [], $halt = false): mixed
    {
        if (! $event instanceof ShouldPublish) {
            return parent::dispatch($event, $payload, $halt);
        }

        $exchange = property_exists($event, 'exchange') ? $event->exchange : class_basename($event);
        $routingKey = method_exists($event, 'routingKey') ? $event->routingKey() : null;
        $messagePayload = array_map(
            function ($property) {
                return $this->formatProperty($property);
            },
            call_user_func('get_object_vars', $event)
        ) + ['event.name' => class_basename($event)];

        // Try direct publish first (fast path)
        try {
            $this->publishDirectly($exchange, $routingKey, $messagePayload);

            Log::info('RabbitMQ: Event published directly', [
                'event' => class_basename($event),
                'exchange' => $exchange,
                'routing_key' => $routingKey,
            ]);

            return null;
        } catch (Throwable $e) {
            Log::warning('RabbitMQ: Direct publish failed, falling back to outbox', [
                'event' => class_basename($event),
                'error' => $e->getMessage(),
            ]);
        }

        // Fallback to outbox on failure (guaranteed delivery)
        if (class_exists('Xerxes\RabbitMQ\Services\OutboxService')) {
            try {
                $outboxService = app('Xerxes\RabbitMQ\Services\OutboxService');
                $message = $outboxService->store($exchange, $routingKey, $messagePayload);

                Log::info('RabbitMQ: Event stored in outbox for retry', [
                    'event' => class_basename($event),
                    'outbox_id' => $message->id ?? null,
                ]);
            } catch (Throwable $e) {
                Log::error('RabbitMQ: Failed to store event in outbox', [
                    'event' => class_basename($event),
                    'error' => $e->getMessage(),
                ]);
                throw $e;
            }
        }

        return null;
    }

    private function publishDirectly(string $exchange, ?string $routingKey, array $payload): void
    {
        /** @var RabbitMQ $rabbitmq */
        $rabbitmq = resolve(RabbitMQ::class);

        $message = $rabbitmq
            ->message()
            ->persistent()
            ->viaExchange($exchange)
            ->withPayload($payload);

        if ($routingKey !== null) {
            $message->route($routingKey);
        }

        $message->publishDirect();
    }

    private function formatProperty($property)
    {
        if ($property instanceof Arrayable) {
            return $property->toArray();
        }

        return $property;
    }
}
