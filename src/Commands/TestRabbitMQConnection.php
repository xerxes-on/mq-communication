<?php

namespace Xerxes\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Xerxes\RabbitMQ\RabbitMQ;

class TestRabbitMQConnection extends Command
{
    protected $signature = 'rabbitmq:test-connection';

    protected $description = 'Test RabbitMQ connection';

    public function handle(): int
    {
        $this->info('Testing RabbitMQ Connection...');
        $this->newLine();

        $this->info('Configuration:');
        $this->info('Host: '.config('rabbitmq.host'));
        $this->info('Port: '.config('rabbitmq.port'));
        $this->info('User: '.config('rabbitmq.user'));
        $this->info('VHost: '.config('rabbitmq.vhost'));
        $this->newLine();

        try {
            $this->info('Attempting to connect...');

            $rabbitmq = app(RabbitMQ::class);

            $rabbitmq
                ->message()
                ->persistent()
                ->viaExchange('test_exchange')
                ->withPayload([
                    'test' => true,
                    'message' => 'Connection test from Laravel',
                    'timestamp' => now()->toIso8601String(),
                ])
                ->publish();

            $this->info('✅ Successfully connected to RabbitMQ!');
            $this->info('Published test message to test_exchange');
            $this->newLine();

            return self::SUCCESS;
        } catch (\Exception $e) {
            $this->error('❌ Failed to connect to RabbitMQ');
            $this->error('Error: '.$e->getMessage());
            $this->newLine();
            $this->error('Please check:');
            $this->error('1. RabbitMQ server is running');
            $this->error('2. Port '.config('rabbitmq.port').' is accessible');
            $this->error('3. Credentials are correct');
            $this->newLine();

            return self::FAILURE;
        }
    }
}
