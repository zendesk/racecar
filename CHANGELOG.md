# Changelog

## Unreleased

## racecar v1.1.0

* Require ruby-kafka v1.0 or higher.
* Add error handling for required libraries (#149).

## racecar v1.0.1

* Add `--without-rails` option to boot consumer without Rails (#139).

## racecar v1.0.0

Unchanged from v0.5.0.

## racecar v0.5.0

* Add support for manually sending heartbeats with `heartbeat` (#105).
* Allow configuring `sasl_over_ssl`.
* Add current directory to `$LOAD_PATH` only when `--require` option is used (#117).
* Support for `ssl_verify_hostname` in the configuration (#120)

## racecar v0.4.2

* Allow configuring `max_bytes` and `max_fetch_queue_size`.

## racecar v0.4.1

* Allow configuring the producer (#77).
* Add support for configuring exponential pause backoff (#76).

## racecar v0.4.0

* Require Kafka 0.10 or higher.
* Support configuring SASL SCRAM authentication (#65).

## racecar v0.3.8

* Change the default `max_wait_time` to 1 second.
* Allow setting the `offset_retention_time` for consumers.
* Allow pausing partitions indefinitely (#63).

## racecar v0.3.7

* Allow setting the key and/or partition key when producing messages.

## racecar v0.3.6

* Allow producing messages (alpha).

## racecar v0.3.5

* Instrument using ActiveSupport::Notifications (#43).
* Add support for SASL.

## racecar v0.3.4

* Use KingKonf for defining configuration variables.
* Allow setting configuration variables through the CLI.
* Make all configuration variables available over the ENV.
* Allow configuring Datadog monitoring.
