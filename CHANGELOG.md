# Changelog

All notable changes to `laravel-rabbitmq-communication` will be documented in this file.

## v2.0.0 - 2025-01-15

### Changed
- **Breaking:** Outbox pattern now tries direct publish first, falls back to database storage on failure (previously stored first, then published)
- Updated `OutboxMessage` model to use `casts()` method instead of `$casts` property
- Replaced hardcoded log channels with configurable `rabbitmq.log-channel` config option
- Updated all classes with proper return type declarations
- Updated dev dependencies to support PHP 8.1+ and Laravel 9-12

### Added
- `OutboxStatus` enum for type-safe status handling
- `ConsumerMode` enum for consumer mode configuration
- `MessageHandler` contract interface for handlers
- `Publisher` contract interface for publishers
- SSL/TLS configuration options for secure RabbitMQ connections
- Configurable outbox table names via `rabbitmq.outbox.table` config
- `consumer_prefetch` config option (previously referenced but undefined)
- Comprehensive test suite with Pest

### Fixed
- Removed hardcoded `telegram` log channel from consumer commands
- Fixed inconsistent outbox behavior between `RabbitMQMessage` and `RabbitMQDispatcher`

## v1.4.0 - 2023-07-19

- Specify heartbeat for connection in env
