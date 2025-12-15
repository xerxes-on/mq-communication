<?php

namespace Xerxes\RabbitMQ;

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Traits\Conditionable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Xerxes\RabbitMQ\Services\OutboxService;

class RabbitMQMessage
{
    use Conditionable;

    private AMQPChannel $channel;

    private array $payload = [];

    private bool $persistent = false;

    private ?bool $useOutbox = null;

    private string $routingKey = '';

    private string $exchange = '';

    /**
     * @var array<string, mixed>
     */
    private array $headers = [];

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    public function withPayload(array $payload = []): RabbitMQMessage
    {
        $this->payload = $payload;

        return $this;
    }

    public function persistent(): RabbitMQMessage
    {
        $this->persistent = true;

        return $this;
    }

    public function route(string $routingKey): RabbitMQMessage
    {
        $this->routingKey = $routingKey;

        return $this;
    }

    public function viaExchange(string $exchange): RabbitMQMessage
    {
        $this->exchange = $exchange;

        return $this;
    }

    /**
     * Enable outbox pattern for guaranteed delivery.
     * Messages are stored in database first, then published.
     */
    public function viaOutbox(bool $enabled = true): RabbitMQMessage
    {
        $this->useOutbox = $enabled;

        return $this;
    }

    /**
     * Disable outbox pattern - publish directly to RabbitMQ.
     */
    public function withoutOutbox(): RabbitMQMessage
    {
        $this->useOutbox = false;

        return $this;
    }

    /**
     * Set custom headers for the message.
     *
     * @param  array<string, mixed>  $headers
     */
    public function withHeaders(array $headers): RabbitMQMessage
    {
        $this->headers = array_merge($this->headers, $headers);

        return $this;
    }

    public function publish(): void
    {
        // Check if outbox is explicitly set, otherwise use config default
        $useOutbox = $this->useOutbox ?? config('rabbitmq.outbox.enabled', true);

        if ($useOutbox) {
            $this->publishViaOutbox();

            return;
        }

        $this->publishDirectly();
    }

    private function publishViaOutbox(): void
    {
        $logChannel = config('rabbitmq.log-channel', config('logging.default'));

        try {
            $this->publishDirectly();

            Log::channel($logChannel)->info('[RabbitMQ] Message published directly', [
                'exchange' => $this->exchange,
                'routing_key' => $this->routingKey,
            ]);
        } catch (\Throwable $exception) {
            $outboxService = app(OutboxService::class);
            $outboxService->store($this->exchange, $this->routingKey, $this->payload);

            Log::channel($logChannel)->warning('[RabbitMQ] Direct publish failed, stored in outbox for retry', [
                'exchange' => $this->exchange,
                'routing_key' => $this->routingKey,
                'error' => $exception->getMessage(),
            ]);
        }
    }

    private function publishDirectly(): void
    {
        $this->channel->basic_publish(
            new AMQPMessage(json_encode($this->payload), $this->properties()),
            $this->exchange,
            $this->routingKey
        );
    }

    private function properties(): array
    {
        $properties = [];

        if ($this->persistent) {
            $properties['delivery_mode'] = AMQPMessage::DELIVERY_MODE_PERSISTENT;
        }

        if (! empty($this->headers)) {
            $properties['application_headers'] = new AMQPTable($this->headers);
        }

        return $properties;
    }
}
