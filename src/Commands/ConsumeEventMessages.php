<?php

namespace Xerxes\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Xerxes\RabbitMQ\RabbitMQ;
use Xerxes\RabbitMQ\Support\EventJobWrapper;

class ConsumeEventMessages extends Command
{
    protected $signature = 'rabbitmq:consume-events';

    protected $description = 'consume events in the rabbitmq';

    /**
     * @var array<string, array<int, array<string, mixed>>>
     */
    private array $eventsByQueue = [];

    private bool $dlqEnabled = false;

    private int $maxRetries = 3;

    /**
     * @var array<int, int>
     */
    private array $retryDelays = [];

    private string $dlqExchange = '';

    private string $retryExchange = '';

    private string $dlqSuffix = '';

    public function __construct()
    {
        parent::__construct();

        $defaultQueue = config('rabbitmq.queue-name', config('app.name'));

        $this->eventsByQueue = collect(config('rabbitmq.event-consumers', []))
            ->map(function (array $event) use ($defaultQueue) {
                return [
                    'exchange' => $event['exchange'] ?? class_basename($event['event'] ?? ''),
                    'exchange_type' => $event['exchange_type'] ?? 'topic',
                    'routing_key' => $event['routing_key'] ?? '',
                    'event' => $event['event'] ?? null,
                    'map_into_event' => $event['map_into'] ?? ($event['event'] ?? null),
                    'handler' => $event['handler'] ?? null,
                    'queue' => $event['queue'] ?? $defaultQueue,
                ];
            })
            ->filter(function (array $event) {
                return $event['handler'] || $event['map_into_event'];
            })
            ->groupBy('queue')
            ->toArray();

        // Load DLQ configuration
        $this->dlqEnabled = (bool) config('rabbitmq.dead_letter.enabled', true);
        $this->maxRetries = (int) config('rabbitmq.dead_letter.max_retries', 3);
        $this->retryDelays = config('rabbitmq.dead_letter.retry_delays', []);
        $this->dlqExchange = config('rabbitmq.dead_letter.exchange', 'dlx.failed');
        $this->retryExchange = config('rabbitmq.dead_letter.retry_exchange', 'dlx.retry');
        $this->dlqSuffix = config('rabbitmq.dead_letter.queue_suffix', '.dlq');
    }

    public function handle(RabbitMQ $rabbitmq): int
    {
        if ($this->eventsByQueue === []) {
            $this->warn('No rabbitmq.event-consumers configured.');
            $this->logWarning('No rabbitmq.event-consumers configured');

            return Command::SUCCESS;
        }

        $this->logInfo('Starting RabbitMQ consumer', [
            'queues' => array_keys($this->eventsByQueue),
            'event_consumers' => collect($this->eventsByQueue)->map(fn ($events) => collect($events)->map(fn ($e) => [
                'exchange' => $e['exchange'],
                'routing_key' => $e['routing_key'],
                'handler' => is_array($e['handler'] ?? null) ? implode('::', $e['handler']) : ($e['handler'] ?? $e['map_into_event'] ?? 'none'),
            ])->toArray())->toArray(),
            'dlq_enabled' => $this->dlqEnabled,
        ]);

        $prefetch = (int) config('rabbitmq.consumer_prefetch', 1);

        // Set up DLQ infrastructure if enabled
        if ($this->dlqEnabled) {
            $this->setupDeadLetterInfrastructure($rabbitmq);
            $this->logInfo('DLQ infrastructure configured', [
                'dlq_exchange' => $this->dlqExchange,
                'retry_exchange' => $this->retryExchange,
                'max_retries' => $this->maxRetries,
            ]);
        }

        $consumer = $rabbitmq
            ->consume()
            ->acknowledge()
            ->receiveWithoutAcknowledgement($prefetch);

        // Set up error handler for DLQ/retry
        if ($this->dlqEnabled) {
            $consumer->onError(function (AMQPMessage $message, \Throwable $exception, array $context) use ($rabbitmq) {
                $this->handleFailedMessage($rabbitmq, $message, $exception, $context);
            });
        }

        foreach ($this->eventsByQueue as $queue => $events) {
            $queueArgs = [];

            // Configure queue with dead letter exchange if enabled
            if ($this->dlqEnabled) {
                $queueArgs['x-dead-letter-exchange'] = $this->dlqExchange;
                $queueArgs['x-dead-letter-routing-key'] = $queue.$this->dlqSuffix;
            }

            $queueBuilder = $rabbitmq
                ->queue()
                ->durable()
                ->name($queue);

            if (! empty($queueArgs)) {
                $queueBuilder->withArguments($queueArgs);
            }

            $queueBuilder->declare();

            foreach ($events as $event) {
                $exchangeBuilder = $rabbitmq
                    ->exchange()
                    ->name($event['exchange'])
                    ->type($event['exchange_type']);

                if (($event['durable'] ?? true) === true) {
                    $exchangeBuilder->durable();
                }

                $exchangeBuilder->declare();

                $queueBuilder->bindTo($event['exchange'], $event['routing_key']);

                $this->logInfo('Queue bound to exchange', [
                    'queue' => $queue,
                    'exchange' => $event['exchange'],
                    'routing_key' => $event['routing_key'],
                ]);
            }

            $consumer->from($queue, function (array $payload, string $routingKey) use ($queue) {
                $this->logInfo('Message received', [
                    'queue' => $queue,
                    'routing_key' => $routingKey,
                    'event' => $payload['event'] ?? ($payload['event.name'] ?? 'unknown'),
                    'payload_keys' => array_keys($payload),
                ]);

                try {
                    $this->fireEvent($queue, $payload, $routingKey);
                    $this->logInfo('Message processed successfully', ['queue' => $queue, 'routing_key' => $routingKey]);
                } catch (\Throwable $e) {
                    $this->logError('Message processing failed', [
                        'queue' => $queue,
                        'routing_key' => $routingKey,
                        'error' => $e->getMessage(),
                        'file' => $e->getFile().':'.$e->getLine(),
                        'trace' => substr($e->getTraceAsString(), 0, 2000),
                    ]);
                    throw $e;
                }
            });
        }

        $this->info('Consumer started. Waiting for messages...');
        $this->logInfo('Consumer started - waiting for messages', [
            'queues' => array_keys($this->eventsByQueue),
        ]);
        $consumer->receive();

        return Command::SUCCESS;
    }

    private function logInfo(string $message, array $context = []): void
    {
        $this->info($message);
        $logChannel = config('rabbitmq.log-channel', config('logging.default'));
        Log::channel($logChannel)->info('[RabbitMQ Consumer] '.$message, $context);
    }

    private function logWarning(string $message, array $context = []): void
    {
        $this->warn($message);
        $logChannel = config('rabbitmq.log-channel', config('logging.default'));
        Log::channel($logChannel)->warning('[RabbitMQ Consumer] '.$message, $context);
    }

    private function logError(string $message, array $context = []): void
    {
        $this->error($message);
        $logChannel = config('rabbitmq.log-channel', config('logging.default'));
        Log::channel($logChannel)->error('[RabbitMQ Consumer] '.$message, $context);
    }

    /**
     * Set up dead letter exchange and queues.
     */
    private function setupDeadLetterInfrastructure(RabbitMQ $rabbitmq): void
    {
        // Declare the main dead letter exchange
        $rabbitmq->exchange()
            ->name($this->dlqExchange)
            ->type('topic')
            ->durable()
            ->declare();

        // Declare the retry exchange
        $rabbitmq->exchange()
            ->name($this->retryExchange)
            ->type('topic')
            ->durable()
            ->declare();

        // Create DLQ and retry queues for each main queue
        foreach ($this->eventsByQueue as $queue => $events) {
            $dlqQueueName = $queue.$this->dlqSuffix;
            $retryQueueName = $queue.'.retry';

            // DLQ for permanently failed messages
            $rabbitmq->queue()
                ->name($dlqQueueName)
                ->durable()
                ->declare()
                ->bindTo($this->dlqExchange, $dlqQueueName);

            // Retry queue with TTL that routes back to original exchange
            foreach ($this->retryDelays as $attempt => $delayMs) {
                $retryQueueWithDelay = $retryQueueName.'.'.$attempt;

                // Get original exchange from events
                $originalExchange = $events[0]['exchange'] ?? '';

                $rabbitmq->queue()
                    ->name($retryQueueWithDelay)
                    ->durable()
                    ->withArguments([
                        'x-message-ttl' => $delayMs,
                        'x-dead-letter-exchange' => $originalExchange,
                        'x-dead-letter-routing-key' => $events[0]['routing_key'] ?? '',
                    ])
                    ->declare()
                    ->bindTo($this->retryExchange, $retryQueueWithDelay);
            }
        }

        $this->info('Dead letter queue infrastructure configured.');
    }

    /**
     * Handle a failed message - retry or send to DLQ.
     */
    private function handleFailedMessage(
        RabbitMQ $rabbitmq,
        AMQPMessage $message,
        \Throwable $exception,
        array $context
    ): void {
        $queue = $context['queue'] ?? 'unknown';
        $payload = $context['payload'] ?? [];
        $routingKey = $context['routing_key'] ?? '';

        // Get retry count from message headers
        $headers = $message->get_properties();
        $applicationHeaders = $headers['application_headers'] ?? new AMQPTable([]);
        $headersArray = $applicationHeaders instanceof AMQPTable
            ? $applicationHeaders->getNativeData()
            : [];

        $retryCount = (int) ($headersArray['x-retry-count'] ?? 0);
        $retryCount++;

        Log::channel(config('rabbitmq.log-channel'))->error('Message processing failed', [
            'queue' => $queue,
            'routing_key' => $routingKey,
            'retry_count' => $retryCount,
            'max_retries' => $this->maxRetries,
            'error' => $exception->getMessage(),
            'trace' => $exception->getTraceAsString(),
        ]);

        // ACK the original message (we'll either retry or send to DLQ)
        $message->ack();

        if ($retryCount <= $this->maxRetries) {
            // Republish to retry queue with delay
            $this->publishToRetryQueue($rabbitmq, $queue, $payload, $routingKey, $retryCount, $exception->getMessage());
            $this->warn("Message will be retried (attempt {$retryCount}/{$this->maxRetries})");
        } else {
            // Max retries exceeded, send to DLQ
            $this->publishToDeadLetterQueue($rabbitmq, $queue, $payload, $routingKey, $retryCount, $exception->getMessage());
            $this->error("Message moved to DLQ after {$this->maxRetries} failed attempts");
        }
    }

    /**
     * Publish message to retry queue with delay.
     */
    private function publishToRetryQueue(
        RabbitMQ $rabbitmq,
        string $originalQueue,
        array $payload,
        string $routingKey,
        int $retryCount,
        string $errorMessage
    ): void {
        $retryQueueRoutingKey = $originalQueue.'.retry.'.$retryCount;

        $enrichedPayload = array_merge($payload, [
            '_retry_metadata' => [
                'original_queue' => $originalQueue,
                'original_routing_key' => $routingKey,
                'retry_count' => $retryCount,
                'last_error' => $errorMessage,
                'retry_at' => now()->toIso8601String(),
            ],
        ]);

        $rabbitmq->message()
            ->viaExchange($this->retryExchange)
            ->route($retryQueueRoutingKey)
            ->withPayload($enrichedPayload)
            ->persistent()
            ->withoutOutbox()
            ->withHeaders([
                'x-retry-count' => $retryCount,
                'x-original-queue' => $originalQueue,
                'x-original-routing-key' => $routingKey,
            ])
            ->publish();
    }

    /**
     * Publish message to dead letter queue.
     */
    private function publishToDeadLetterQueue(
        RabbitMQ $rabbitmq,
        string $originalQueue,
        array $payload,
        string $routingKey,
        int $retryCount,
        string $errorMessage
    ): void {
        $dlqRoutingKey = $originalQueue.$this->dlqSuffix;

        $enrichedPayload = array_merge($payload, [
            '_dlq_metadata' => [
                'original_queue' => $originalQueue,
                'original_routing_key' => $routingKey,
                'total_attempts' => $retryCount,
                'last_error' => $errorMessage,
                'failed_at' => now()->toIso8601String(),
            ],
        ]);

        $rabbitmq->message()
            ->viaExchange($this->dlqExchange)
            ->route($dlqRoutingKey)
            ->withPayload($enrichedPayload)
            ->persistent()
            ->withoutOutbox()
            ->publish();
    }

    /**
     * Match routing key using RabbitMQ wildcard syntax.
     * Converts # (multi-word) and * (single-word) to Str::is compatible patterns.
     */
    private function matchesRoutingKey(string $pattern, string $routingKey): bool
    {
        // Convert RabbitMQ wildcards to Str::is patterns
        // # matches zero or more words (including dots) -> *
        // * matches exactly one word -> [^.]+
        $strIsPattern = str_replace('#', '*', $pattern);

        return Str::is($strIsPattern, $routingKey);
    }

    private function fireEvent(string $queue, array $payload, string $routingKey): void
    {
        $events = $this->eventsByQueue[$queue] ?? [];

        $eventConfig = collect($events)->first(function (array $event) use ($payload, $routingKey) {
            if ($event['routing_key'] !== '' && ! $this->matchesRoutingKey($event['routing_key'], $routingKey)) {
                return false;
            }

            if (! $event['event']) {
                return true;
            }

            if (! isset($payload['event.name'])) {
                return false;
            }

            return $payload['event.name'] === class_basename($event['event']);
        });

        if (! $eventConfig) {
            $this->logWarning('Message consumed without matching handler', [
                'queue' => $queue,
                'routing_key' => $routingKey,
                'available_routing_keys' => collect($events)->pluck('routing_key')->toArray(),
            ]);

            return;
        }

        if ($handler = $eventConfig['handler']) {
            $handlerName = is_array($handler) ? implode('::', $handler) : (is_string($handler) ? $handler : 'closure');
            $this->logInfo('Calling handler', [
                'queue' => $queue,
                'routing_key' => $routingKey,
                'handler' => $handlerName,
            ]);

            try {
                // Handle array format [Class::class, 'method'] - resolve instance first
                if (is_array($handler) && count($handler) === 2 && is_string($handler[0])) {
                    $instance = app()->make($handler[0]);
                    $method = $handler[1];
                    app()->call([$instance, $method], [
                        'payload' => $payload,
                        'routingKey' => $routingKey,
                    ]);
                } else {
                    app()->call($handler, [
                        'payload' => $payload,
                        'routingKey' => $routingKey,
                    ]);
                }

                $this->logInfo('Handler executed successfully', [
                    'queue' => $queue,
                    'routing_key' => $routingKey,
                    'handler' => $handlerName,
                ]);
            } catch (\Throwable $exception) {
                $this->logError('Handler execution failed', [
                    'queue' => $queue,
                    'routing_key' => $routingKey,
                    'handler' => $handlerName,
                    'error' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                ]);
                throw $exception;
            }

            return;
        }

        $eventClass = $eventConfig['map_into_event'];
        if (! $eventClass) {
            return;
        }

        $mode = config('rabbitmq.event-consumer-mode', 'sync');

        $resolvedPayload = array_merge([
            'routingKey' => $routingKey,
            'queue' => $queue,
        ], $payload);

        if ($mode === 'sync') {
            event(resolve($eventClass, $resolvedPayload));

            return;
        }

        if ($mode === 'kind-sync') {
            try {
                event(resolve($eventClass, $resolvedPayload));
            } catch (\Exception $exception) {
                $this->logError('Could not dispatch event consumed from rabbitmq', [
                    'consumed_event' => $payload['event.name'] ?? null,
                    'routing_key' => $routingKey,
                    'was_going_to_map_into' => $eventClass,
                    'error_message' => $exception->getMessage(),
                    'error_trace' => $exception->getTraceAsString(),
                ]);
            }

            return;
        }

        if ($mode === 'job') {
            EventJobWrapper::dispatch($eventClass, $resolvedPayload);
        }
    }
}
