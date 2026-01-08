# Laravel RabbitMQ Communication

[![Latest Version](https://img.shields.io/packagist/v/xerxes/laravel-rabbitmq-communication.svg?style=flat-square)](https://packagist.org/packages/xerxes/laravel-rabbitmq-communication)
[![License](https://img.shields.io/packagist/l/xerxes/laravel-rabbitmq-communication.svg?style=flat-square)](LICENSE.md)

A Laravel package for RabbitMQ messaging with support for the outbox pattern, event consumers, dead letter queues, and automatic retry mechanisms.

## Features

- **Direct Publish with Outbox Fallback** - Messages are published directly; if RabbitMQ is unavailable, they're stored for later retry
- **Event Consumers** - Consume messages from multiple queues with configurable handlers
- **Dead Letter Queue (DLQ)** - Automatic retry with exponential backoff, failed messages go to DLQ
- **Type-Safe Enums** - PHP 8.1+ enums for status and mode handling
- **Fully Configurable** - Environment-based configuration for all settings

## Requirements

- PHP 8.1+
- Laravel 9, 10, 11, or 12
- RabbitMQ Server
- ext-json

## Installation

```bash
composer require xerxes/laravel-rabbitmq-communication
```

Publish the configuration:

```bash
php artisan vendor:publish --tag=laravel-rabbitmq-communication-config
```

Run migrations:

```bash
php artisan migrate
```

## Configuration

### Environment Variables

```env
# Connection Settings
RABBITMQ_HOST=127.0.0.1
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_VHOST=/

# Outbox Pattern (always enabled as fallback)
RABBITMQ_OUTBOX_CONNECTION=outbox

# Dead Letter Queue
RABBITMQ_DLQ_ENABLED=true
RABBITMQ_DLQ_EXCHANGE=dlx.failed
RABBITMQ_DLQ_MAX_RETRIES=3

# Consumer Settings
RABBITMQ_CONSUMER_MODE=sync
RABBITMQ_CONSUMER_PREFETCH=10
RABBITMQ_LOG_CHANNEL=stack
```

## Publishing Messages

### Basic Publishing

```php
use Xerxes\RabbitMQ\RabbitMQ;

app(RabbitMQ::class)
    ->message()
    ->viaExchange('my.exchange')
    ->route('my.routing.key')
    ->withPayload(['event' => 'user.created', 'data' => [...]])
    ->persistent()
    ->publish();
```

### Publishing with Headers

```php
app(RabbitMQ::class)
    ->message()
    ->viaExchange('my.exchange')
    ->route('my.routing.key')
    ->withPayload($payload)
    ->withHeaders(['x-custom-header' => 'value'])
    ->persistent()
    ->publish();
```

### Outbox Pattern (Guaranteed Delivery)

The outbox pattern is always enabled as an automatic fallback and cannot be disabled. Messages follow this flow:

1. **Try direct publish** - Message is sent immediately to RabbitMQ
2. **On failure** - Message is automatically stored in the database outbox table
3. **Outbox worker** - Processes pending messages and retries publishing

```php
app(RabbitMQ::class)
    ->message()
    ->viaExchange('my.exchange')
    ->route('my.routing.key')
    ->withPayload($payload)
    ->publish();
```

Process outbox messages:

```bash
php artisan outbox:work
```

### Using Laravel Events

Implement the `ShouldPublish` interface to automatically publish events:

```php
use Xerxes\RabbitMQ\Support\ShouldPublish;

class UserCreated implements ShouldPublish
{
    public string $exchange = 'user.events';

    public function __construct(
        public User $user
    ) {}

    public function routingKey(): string
    {
        return 'user.created';
    }
}

// Dispatch normally - automatically published to RabbitMQ
event(new UserCreated($user));
```

## Consuming Messages

### Configuration

Define event consumers in `config/rabbitmq.php`:

```php
'event-consumers' => [
    [
        'exchange' => 'user.events',
        'exchange_type' => 'topic',
        'routing_key' => 'user.#',
        'handler' => [\App\Handlers\UserHandler::class, 'handle'],
        'queue' => 'my-app.users',
    ],
],
```

### Handler Class

Your handler should implement the `MessageHandler` interface:

```php
use Xerxes\RabbitMQ\Contracts\MessageHandler;

class UserHandler implements MessageHandler
{
    public function handle(array $payload, ?string $routingKey = null): void
    {
        // Process the message
        Log::info('Processing user event', [
            'routing_key' => $routingKey,
            'payload' => $payload,
        ]);
    }
}
```

### Running the Consumer

```bash
php artisan rabbitmq:consume-events
```

### Consumer Modes

Configure how consumed events are dispatched:

- **sync**: Events fired synchronously. Errors stop the consumer (unless DLQ is enabled).
- **kind-sync**: Events fired synchronously. Errors are logged but don't stop the consumer.
- **job**: Events dispatched via Laravel queue jobs.

### Message Acknowledgment

Message acknowledgment is always required and cannot be disabled:

- **On success**: Message is acknowledged and removed from the queue
- **On failure**: Message is rejected and requeued for retry
- **With DLQ enabled**: After max retries, message is moved to the dead letter queue

## Dead Letter Queue (DLQ)

### How It Works

1. Message fails processing
2. Republished to retry queue with delay
3. After TTL, routed back to original queue
4. After max retries (default: 3), moved to dead letter queue
5. Failed messages can be manually reprocessed

### Retry Flow

```
Message fails → Retry Queue (5s delay) → Original Queue
     ↓ fails again
Retry Queue (30s delay) → Original Queue
     ↓ fails again
Retry Queue (2min delay) → Original Queue
     ↓ fails again (max retries exceeded)
Dead Letter Queue (.dlq)
```

### Reprocessing Failed Messages

```bash
# Reprocess up to 100 messages
php artisan rabbitmq:reprocess-dlq my-app.orders

# Limit the number
php artisan rabbitmq:reprocess-dlq my-app.orders --limit=50

# Dry run
php artisan rabbitmq:reprocess-dlq my-app.orders --dry-run
```

## Available Commands

| Command | Description |
|---------|-------------|
| `rabbitmq:consume-events` | Start consuming messages from configured queues |
| `outbox:work` | Run outbox worker (continuous) |
| `outbox:process` | Process outbox messages once |
| `rabbitmq:declare-exchanges` | Declare configured exchanges |
| `rabbitmq:test-connection` | Test RabbitMQ connection |
| `rabbitmq:reprocess-dlq {queue}` | Reprocess messages from dead letter queue |

## Testing

```bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
