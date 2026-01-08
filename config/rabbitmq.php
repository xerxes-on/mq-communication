<?php

return [
    /*
    |--------------------------------------------------------------------------
    | RabbitMQ Connection Settings
    |--------------------------------------------------------------------------
    |
    | Configure the connection to your RabbitMQ server. These settings are
    | passed directly to the php-amqplib connection.
    |
    */

    'host' => env('RABBITMQ_HOST', '127.0.0.1'),
    'port' => (int) env('RABBITMQ_PORT', 5672),
    'user' => env('RABBITMQ_USER', 'guest'),
    'password' => env('RABBITMQ_PASS', 'guest'),
    'vhost' => env('RABBITMQ_VHOST', '/'),
    'insist' => (bool) env('RABBITMQ_INSIST', false),
    'login_method' => env('RABBITMQ_LOGIN_METHOD', 'AMQPLAIN'),
    'login_response' => env('RABBITMQ_LOGIN_RESPONSE'),
    'locale' => env('RABBITMQ_LOCALE', 'en_US'),
    'connection_timeout' => (float) env('RABBITMQ_CONNECTION_TIMEOUT', 3.0),
    'read_write_timeout' => (float) env('RABBITMQ_READ_WRITE_TIMEOUT', 3.0),
    'context' => env('RABBITMQ_CONTEXT'),
    'keepalive' => (bool) env('RABBITMQ_KEEPALIVE', false),
    'heartbeat' => (int) env('RABBITMQ_HEARTBEAT', 0),
    'channel_rpc_timeout' => (float) env('RABBITMQ_CHANNEL_RPC_TIMEOUT', 0.0),

    /*
    |--------------------------------------------------------------------------
    | SSL/TLS Settings
    |--------------------------------------------------------------------------
    |
    | Configure SSL/TLS for secure connections to RabbitMQ.
    |
    */

    'ssl' => [
        'enabled' => (bool) env('RABBITMQ_SSL_ENABLED', false),
        'cafile' => env('RABBITMQ_SSL_CAFILE'),
        'local_cert' => env('RABBITMQ_SSL_LOCAL_CERT'),
        'local_key' => env('RABBITMQ_SSL_LOCAL_KEY'),
        'verify_peer' => (bool) env('RABBITMQ_SSL_VERIFY_PEER', true),
        'passphrase' => env('RABBITMQ_SSL_PASSPHRASE'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Outbox Pattern Settings
    |--------------------------------------------------------------------------
    |
    | The outbox pattern provides guaranteed message delivery and is always
    | enabled as an automatic fallback. When publishing a message:
    |
    | 1. Direct publish to RabbitMQ is attempted first
    | 2. If RabbitMQ is unavailable, the message is stored in the outbox database
    | 3. The outbox worker (outbox:work) retries pending messages
    |
    | This behavior cannot be disabled to ensure message delivery guarantees.
    |
    */

    'outbox' => [
        'connection' => env('RABBITMQ_OUTBOX_CONNECTION', 'outbox'),
        'table' => env('RABBITMQ_OUTBOX_TABLE', 'outbox_messages'),
        'failed_table' => env('RABBITMQ_OUTBOX_FAILED_TABLE', 'outbox_failed_messages'),
        'exchanges' => [], // Filter by exchanges for shared outbox databases
    ],

    /*
    |--------------------------------------------------------------------------
    | Event Consumers Configuration
    |--------------------------------------------------------------------------
    |
    | Define which events to consume from RabbitMQ. Each consumer can either
    | use a handler (callable) or map to a Laravel event.
    |
    | Example:
    | [
    |     'exchange' => 'user.events',
    |     'exchange_type' => 'topic',
    |     'routing_key' => 'user.created',
    |     'handler' => [App\Handlers\UserHandler::class, 'handle'],
    |     'queue' => 'my-app.users',
    | ]
    |
    */

    'event-consumers' => [
        // Example:
        // [
        //     'exchange' => 'user.events',
        //     'exchange_type' => 'topic',
        //     'routing_key' => 'user.#',
        //     'handler' => [\App\Handlers\UserHandler::class, 'handle'],
        //     'queue' => 'my-app.users',
        // ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Consumer Settings
    |--------------------------------------------------------------------------
    |
    | Configure consumer behavior including mode and prefetch count.
    |
    | Modes:
    | - sync: Events fired synchronously, errors stop the consumer
    | - kind-sync: Events fired synchronously, errors logged but consumer continues
    | - job: Events dispatched via Laravel queue jobs
    |
    | Note: Message acknowledgment is always required. On successful processing,
    | messages are acknowledged. On failure, messages are requeued for retry
    | (or sent to DLQ if configured and max retries exceeded).
    |
    */

    'event-consumer-mode' => env('RABBITMQ_CONSUMER_MODE', 'sync'),
    'consumer_prefetch' => (int) env('RABBITMQ_CONSUMER_PREFETCH', 1),
    'queue-name' => env('RABBITMQ_QUEUE_NAME', 'default'),

    /*
    |--------------------------------------------------------------------------
    | Logging
    |--------------------------------------------------------------------------
    |
    | Configure the log channel for RabbitMQ operations.
    |
    */

    'log-channel' => env('RABBITMQ_LOG_CHANNEL', env('LOG_CHANNEL', 'stack')),

    /*
    |--------------------------------------------------------------------------
    | Dead Letter Queue (DLQ) Configuration
    |--------------------------------------------------------------------------
    |
    | When a message fails processing:
    | 1. It's retried up to max_retries times with exponential backoff
    | 2. After max retries exhausted, moved to dead letter queue
    |
    */

    'dead_letter' => [
        'enabled' => (bool) env('RABBITMQ_DLQ_ENABLED', true),
        'exchange' => env('RABBITMQ_DLQ_EXCHANGE', 'dlx.failed'),
        'exchange_type' => 'topic',
        'queue_suffix' => '.dlq',
        'max_retries' => (int) env('RABBITMQ_DLQ_MAX_RETRIES', 3),
        'retry_delays' => [
            1 => 5000,      // 1st retry: 5 seconds
            2 => 30000,     // 2nd retry: 30 seconds
            3 => 120000,    // 3rd retry: 2 minutes
        ],
        'retry_exchange' => env('RABBITMQ_RETRY_EXCHANGE', 'dlx.retry'),
    ],
];
