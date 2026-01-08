<?php

namespace Xerxes\RabbitMQ;

use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQConsumer
{
    private AMQPChannel $channel;

    private int $qos = 1;

    private ?Closure $errorHandler = null;

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    /**
     * Set the prefetch count (QoS) for the consumer.
     */
    public function prefetch(int $count): RabbitMQConsumer
    {
        $this->qos = $count;

        return $this;
    }

    /**
     * Set a custom error handler for failed message processing.
     *
     * @param  Closure(AMQPMessage, \Throwable, array): void  $handler
     */
    public function onError(Closure $handler): RabbitMQConsumer
    {
        $this->errorHandler = $handler;

        return $this;
    }

    /**
     * Consume messages from a queue.
     *
     * Messages are always consumed with manual acknowledgment required.
     * On successful processing, message is acknowledged.
     * On failure, message is rejected and requeued (or sent to DLQ if configured).
     */
    public function from(string $queue, callable $handle): RabbitMQConsumer
    {
        $this->channel->basic_qos(null, $this->qos, null);

        $this->channel->basic_consume(
            $queue,
            '',
            false,
            false, // no_ack=false: always require manual acknowledgment
            false,
            false,
            function (AMQPMessage $message) use ($handle, $queue) {
                $payload = json_decode($message->getBody(), true);
                $routingKey = $message->getRoutingKey();

                try {
                    call_user_func_array($handle, [$payload, $routingKey]);
                    $message->ack();
                } catch (\Throwable $exception) {
                    if ($this->errorHandler !== null) {
                        call_user_func($this->errorHandler, $message, $exception, [
                            'queue' => $queue,
                            'payload' => $payload,
                            'routing_key' => $routingKey,
                        ]);
                    } else {
                        // Reject and requeue the message for retry
                        // If DLQ is configured, message will be sent there after max retries
                        $message->nack(true);
                        throw $exception;
                    }
                }
            }
        );

        return $this;
    }

    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }

    public function receive(): void
    {
        while ($this->channel->is_open()) {
            $this->channel->wait();
        }
    }

    public function __destruct()
    {
        $this->channel->close();
    }
}
