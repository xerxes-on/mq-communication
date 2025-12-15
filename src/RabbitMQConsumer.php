<?php

namespace Xerxes\RabbitMQ;

use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQConsumer
{
    private AMQPChannel $channel;

    private int $qos = 1;

    private bool $acknowledge = false;

    private ?Closure $errorHandler = null;

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    public function receiveWithoutAcknowledgement(int $numberOfMessages): RabbitMQConsumer
    {
        $this->qos = $numberOfMessages;

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

    public function from(string $queue, callable $handle): RabbitMQConsumer
    {
        $this->channel->basic_qos(null, $this->qos, null);

        $this->channel->basic_consume(
            $queue,
            '',
            false,
            ! $this->acknowledge,
            false,
            false,
            function (AMQPMessage $message) use ($handle, $queue) {
                $payload = json_decode($message->getBody(), true);
                $routingKey = $message->getRoutingKey();

                try {
                    call_user_func_array($handle, [$payload, $routingKey]);

                    if ($this->acknowledge) {
                        $message->ack();
                    }
                } catch (\Throwable $exception) {
                    if ($this->errorHandler !== null) {
                        call_user_func($this->errorHandler, $message, $exception, [
                            'queue' => $queue,
                            'payload' => $payload,
                            'routing_key' => $routingKey,
                        ]);
                    } else {
                        // Default behavior: nack without requeue (sends to DLQ if configured)
                        if ($this->acknowledge) {
                            $message->nack(false);
                        }
                        throw $exception;
                    }
                }
            }
        );

        return $this;
    }

    public function acknowledge(): RabbitMQConsumer
    {
        $this->acknowledge = true;

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
