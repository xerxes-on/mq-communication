<?php

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
        $connection = config('rabbitmq.outbox.connection', 'outbox');

        if (Schema::connection($connection)->hasTable('outbox_messages')) {
            return;
        }

        Schema::connection($connection)->create('outbox_messages', function (Blueprint $table) {
            $table->id();
            $table->string('exchange');
            $table->string('routing_key')->nullable();
            $table->json('payload');
            $table->string('status')->default('pending');
            $table->timestamp('published_at')->nullable();
            $table->integer('retry_count')->default(0);
            $table->text('error_message')->nullable();
            $table->timestamps();

            $table->index(['status', 'created_at']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        $connection = config('rabbitmq.outbox.connection', 'outbox');
        Schema::connection($connection)->dropIfExists('outbox_messages');
    }
};
