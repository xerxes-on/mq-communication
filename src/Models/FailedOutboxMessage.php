<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Models;

use Illuminate\Database\Eloquent\Model;

class FailedOutboxMessage extends Model
{
    protected $connection = 'outbox';

    protected $table = 'outbox_failed_messages';

    protected $fillable = [
        'outbox_message_id',
        'exchange',
        'routing_key',
        'payload',
        'error_message',
        'failed_at',
    ];

    protected $casts = [
        'payload' => 'array',
        'failed_at' => 'immutable_datetime',
        'created_at' => 'immutable_datetime',
        'updated_at' => 'immutable_datetime',
    ];
}
