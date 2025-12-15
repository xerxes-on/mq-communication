<?php

declare(strict_types=1);

namespace Xerxes\RabbitMQ\Enums;

enum ConsumerMode: string
{
    case Sync = 'sync';
    case KindSync = 'kind-sync';
    case Job = 'job';
}
