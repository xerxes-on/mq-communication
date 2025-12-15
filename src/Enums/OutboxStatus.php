<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Enums;

enum OutboxStatus: string
{
    case Pending = 'pending';
    case Failed = 'failed';
}
