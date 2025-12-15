<?php

namespace Xerxes\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Xerxes\RabbitMQ\RabbitMQ;
use Xerxes\RabbitMQ\Services\OutboxService;

class OutboxWorker extends Command
{
    protected $signature = 'outbox:work {--sleep=1 : Seconds to sleep when no messages} {--limit=100 : Maximum messages to process per iteration}';

    protected $description = 'Run outbox worker to continuously process pending messages';

    public function __construct(
        private OutboxService $outboxService,
        private RabbitMQ $rabbitmq
    ) {
        parent::__construct();
    }

    public function handle(): void
    {
        $this->info('Outbox worker started. Press Ctrl+C to stop.');

        $iterations = 0;
        $maxIterationsBeforeRestart = 1000; // Restart after 1000 iterations to free memory

        while (true) {
            $limit = (int) $this->option('limit');
            $messages = $this->outboxService->getPendingMessages($limit);

            if ($messages->isEmpty()) {
                sleep((int) $this->option('sleep'));

                // Clear query log to prevent memory buildup during idle time
                \DB::connection($this->outboxService->getConnection())->disableQueryLog();

                continue;
            }

            $this->info("Processing {$messages->count()} messages...");

            $successCount = 0;
            $failureCount = 0;

            foreach ($messages as $message) {
                $this->line("Processing message ID {$message->id} - Exchange: {$message->exchange}, Routing: {$message->routing_key}");

                try {
                    $messageBuilder = $this->rabbitmq
                        ->message()
                        ->persistent()
                        ->viaExchange($message->exchange)
                        ->withPayload($message->payload);

                    if ($message->routing_key) {
                        $messageBuilder->route($message->routing_key);
                    }

                    $messageBuilder->withoutOutbox()->publish();

                    $this->outboxService->markAsPublished($message);

                    $successCount++;
                } catch (\Throwable $e) {
                    $this->error("Failed to publish message ID {$message->id}: {$e->getMessage()}");
                    $this->outboxService->markAsFailed($message, $e->getMessage());
                    $failureCount++;
                }

                // Free the model from memory after processing
                unset($message);
            }

            $this->comment("Processed {$messages->count()} messages. Success: {$successCount}, Failed: {$failureCount}.");

            // Free the collection from memory
            unset($messages);

            // Periodically run garbage collection
            $iterations++;
            if ($iterations % 100 === 0) {
                gc_collect_cycles();
                $memoryUsage = round(memory_get_usage(true) / 1024 / 1024, 2);
                $this->comment("Memory usage: {$memoryUsage} MB");
            }

            // Restart worker after max iterations to completely free memory
            if ($iterations >= $maxIterationsBeforeRestart) {
                $this->info("Restarting worker after {$iterations} iterations to free memory...");
                break;
            }
        }
    }
}
