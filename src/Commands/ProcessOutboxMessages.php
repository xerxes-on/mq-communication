<?php

namespace Xerxes\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Xerxes\RabbitMQ\RabbitMQ;
use Xerxes\RabbitMQ\Services\OutboxService;

class ProcessOutboxMessages extends Command
{
    protected $signature = 'outbox:process {--limit=100 : Maximum messages to process per run}';

    protected $description = 'Process pending outbox messages and publish to RabbitMQ';

    public function __construct(
        private OutboxService $outboxService,
        private RabbitMQ $rabbitmq
    ) {
        parent::__construct();
    }

    public function handle(): void
    {
        $limit = (int) $this->option('limit');
        $messages = $this->outboxService->getPendingMessages($limit);

        if ($messages->isEmpty()) {
            $this->info('No pending messages to process.');

            return;
        }

        $this->info("Processing {$messages->count()} messages...");

        $successCount = 0;
        $failureCount = 0;

        foreach ($messages as $message) {
            $this->info("Processing message ID {$message->id} for exchange: {$message->exchange}");

            try {
                $messageBuilder = $this->rabbitmq
                    ->message()
                    ->persistent()
                    ->viaExchange($message->exchange)
                    ->withPayload($message->payload);

                if ($message->routing_key) {
                    $messageBuilder->route($message->routing_key);
                }

                $messageBuilder->publish();

                $this->outboxService->markAsPublished($message);
                $successCount++;

                $this->comment("Message ID {$message->id} published successfully.");
            } catch (\Throwable $e) {
                $this->outboxService->markAsFailed($message, $e->getMessage());
                $failureCount++;

                $this->error("Failed to publish message ID {$message->id}: {$e->getMessage()}");
            }
        }

        $this->comment("Processed {$messages->count()} messages. Success: {$successCount}, Failed: {$failureCount}.");
    }
}
