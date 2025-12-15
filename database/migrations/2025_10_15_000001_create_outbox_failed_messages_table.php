<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        if (Schema::connection('outbox')->hasTable('outbox_failed_messages')) {
            return;
        }

        Schema::connection('outbox')->create('outbox_failed_messages', function (Blueprint $table): void {
            $table->id();
            $table->unsignedBigInteger('outbox_message_id')->nullable();
            $table->string('exchange');
            $table->string('routing_key')->nullable();
            $table->json('payload');
            $table->text('error_message')->nullable();
            $table->timestamp('failed_at');
            $table->timestamps();

            $table->index(['failed_at']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::connection('outbox')->dropIfExists('outbox_failed_messages');
    }
};
