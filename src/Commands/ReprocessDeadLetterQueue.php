<?php

namespace Xerxes\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use PhpAmqpLib\Message\AMQPMessage;
use Xerxes\RabbitMQ\RabbitMQ;

class ReprocessDeadLetterQueue extends Command
{
    protected $signature = 'rabbitmq:reprocess-dlq
                            {queue : The original queue name (DLQ suffix will be added)}
                            {--limit=100 : Maximum number of messages to reprocess}
                            {--dry-run : Show what would be reprocessed without actually doing it}';

    protected $description = 'Reprocess messages from a dead letter queue back to the original queue';

    public function handle(RabbitMQ $rabbitmq): int
    {
        $originalQueue = $this->argument('queue');
        $limit = (int) $this->option('limit');
        $dryRun = (bool) $this->option('dry-run');

        $dlqSuffix = config('rabbitmq.dead_letter.queue_suffix', '.dlq');
        $dlqQueueName = $originalQueue.$dlqSuffix;

        $this->info("Reprocessing messages from: {$dlqQueueName}");

        if ($dryRun) {
            $this->warn('DRY RUN MODE - No messages will be moved');
        }

        // Get the event consumer config for this queue to find the original exchange
        $eventConsumers = collect(config('rabbitmq.event-consumers', []))
            ->filter(fn ($event) => ($event['queue'] ?? config('rabbitmq.queue-name')) === $originalQueue)
            ->first();

        if (! $eventConsumers) {
            $this->error("No event consumer found for queue: {$originalQueue}");

            return Command::FAILURE;
        }

        $originalExchange = $eventConsumers['exchange'] ?? '';
        $originalRoutingKey = $eventConsumers['routing_key'] ?? '';

        $this->info("Will republish to exchange: {$originalExchange} with routing key: {$originalRoutingKey}");

        $consumer = $rabbitmq->consume();
        $channel = $consumer->getChannel();

        $processed = 0;
        $channel->basic_qos(null, 1, null);

        while ($processed < $limit) {
            /** @var AMQPMessage|null $message */
            $message = $channel->basic_get($dlqQueueName);

            if ($message === null) {
                $this->info('No more messages in DLQ');
                break;
            }

            $body = $message->getBody();
            $payload = json_decode($body, true);

            // Remove DLQ metadata before reprocessing
            if (isset($payload['_dlq_metadata'])) {
                unset($payload['_dlq_metadata']);
            }
            if (isset($payload['_retry_metadata'])) {
                unset($payload['_retry_metadata']);
            }

            if ($dryRun) {
                $this->line('Would reprocess: '.substr($body, 0, 100).'...');
                $message->nack(true); // Requeue in dry-run mode
            } else {
                // Republish to original exchange
                $rabbitmq->message()
                    ->viaExchange($originalExchange)
                    ->route($originalRoutingKey)
                    ->withPayload($payload)
                    ->persistent()
                    ->publishDirect();

                // ACK the DLQ message
                $message->ack();

                Log::info('Reprocessed message from DLQ', [
                    'dlq_queue' => $dlqQueueName,
                    'original_exchange' => $originalExchange,
                    'routing_key' => $originalRoutingKey,
                ]);
            }

            $processed++;
        }

        $this->info("Processed {$processed} messages");

        return Command::SUCCESS;
    }
}
