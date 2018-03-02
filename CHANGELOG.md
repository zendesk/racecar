# Changelog

## Unreleased

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
