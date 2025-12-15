<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ;

use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Queue\Factory as QueueFactoryContract;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\ServiceProvider;
use Xerxes\RabbitMQ\Commands\ConsumeEventMessages;
use Xerxes\RabbitMQ\Commands\DeclareEventExchanges;
use Xerxes\RabbitMQ\Commands\OutboxWorker;
use Xerxes\RabbitMQ\Commands\ProcessOutboxMessages;
use Xerxes\RabbitMQ\Commands\ReprocessDeadLetterQueue;
use Xerxes\RabbitMQ\Commands\TestRabbitMQConnection;
use Xerxes\RabbitMQ\Contracts\Publisher;
use Xerxes\RabbitMQ\Services\OutboxService;

class RabbitMQServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(RabbitMQ::class, function (): RabbitMQ {
            return new RabbitMQ;
        });

        $this->app->singleton(OutboxService::class, function (): OutboxService {
            return new OutboxService;
        });

        $this->app->bind(Publisher::class, RabbitMQ::class);

        $this->app->extend('events', function (Dispatcher $dispatcher, Container $app): RabbitMQDispatcher {
            return (new RabbitMQDispatcher($app))->setQueueResolver(function () use ($app) {
                return $app->make(QueueFactoryContract::class);
            });
        });

        $this->mergeConfigFrom(
            __DIR__.'/../config/rabbitmq.php',
            'rabbitmq'
        );

        $this->registerOutboxDatabaseConnection();
    }

    protected function registerOutboxDatabaseConnection(): void
    {
        $this->app->booted(function () {
            $connectionName = config('rabbitmq.outbox.connection', 'outbox');

            // Only register if the connection doesn't already exist
            if (config("database.connections.{$connectionName}") === null) {
                config([
                    "database.connections.{$connectionName}" => [
                        'driver' => 'pgsql',
                        'host' => env('OUTBOX_DB_HOST', '127.0.0.1'),
                        'port' => env('OUTBOX_DB_PORT', '5432'),
                        'database' => env('OUTBOX_DB_DATABASE', 'mq_outbox'),
                        'username' => env('OUTBOX_DB_USERNAME', 'root'),
                        'password' => env('OUTBOX_DB_PASSWORD', ''),
                        'charset' => 'utf8',
                        'prefix' => '',
                        'prefix_indexes' => true,
                        'search_path' => 'public',
                        'sslmode' => 'prefer',
                    ],
                ]);
            }
        });
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                DeclareEventExchanges::class,
                ConsumeEventMessages::class,
                TestRabbitMQConnection::class,
                ProcessOutboxMessages::class,
                OutboxWorker::class,
                ReprocessDeadLetterQueue::class,
            ]);

            $this->loadMigrationsFrom(__DIR__.'/../database/migrations');
        }

        $this->publishes([
            __DIR__.'/../config/rabbitmq.php' => config_path('rabbitmq.php'),
        ], 'laravel-rabbitmq-communication-config');

        $this->publishes([
            __DIR__.'/../database/migrations' => database_path('migrations'),
        ], 'laravel-rabbitmq-communication-migrations');
    }
}
