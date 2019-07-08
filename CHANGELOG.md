# Changelog

## Unreleased

* Replace `ruby-kafka` with `rdkafka-ruby`
* Removed config option `sasl_over_ssl`
* [Racecar::Consumer] Do not pause consuming partitions on exception
* [Racecar::Consumer] `topic`, `payload` and `key` are mandadory to method `produce`
* [Racecar::Consumer] `process_batch` retrieves an array of messages instead of batch object
* [Racecar::Consumer] Remove `offset_retention_time`
* [Racecar::Consumer] Allow providing `additional_config` for subscriptions
* [Racecar::Consumer] Provide access to `producer` and `consumer`
* [Racecar::Consumer] Enforce delivering messages with method `deliver!`
* [Racecar::Consumer] instead of raising when a partition EOF is reached, the result can be queried through `consumer.last_poll_read_partition_eof?`
* [Racecar::Config] Remove `offset_retention_time`, `connect_timeout` and `offset_commit_threshold`
* [Racecar::Config] Pass config to `rdkafka-ruby` via `producer` and `consumer`
* [Racecar::Config] Replace `max_fetch_queue_size` with `min_message_queue_size`
* [Racecar::Config] Add `synchronous_commits` to control blocking of `consumer.commit` (default `false`)
* [Racecar::Config] Add `security_protocol` to control protocol between client and broker
* [Racecar::Config] SSL configuration via `ssl_ca_location`, `ssl_crl_location`, `ssl_keystore_location` and `ssl_keystore_password`
* [Racecar::Config] SASL configuration via `sasl_mechanism`, `sasl_kerberos_service_name`, `sasl_kerberos_principal`, `sasl_kerberos_kinit_cmd`, `sasl_kerberos_keytab`, `sasl_kerberos_min_time_before_relogin`, `sasl_username` and `sasl_password`
* [Instrumentation] `produce_message.racecar` sent whenever a produced message is queued. Payload includes `topic`, `key`, `value` and `create_time`.
* [Instrumentation] `acknowledged_message.racecar` send whenever a produced message was successfully received by Kafka. Payload includes `offset` and `partition`, but no message details.
* [Instrumentation] `rdkafka-ruby` does not yet provide instrumentation [rdkafka-ruby#54](https://github.com/appsignal/rdkafka-ruby/issues/54)
* [Instrumentation] if processors define a `statistics_callback`, it will be called once every second for every subscription or producer connection. The first argument will be a Hash, for contents see [librdkafka STATISTICS.md](https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md)
* Add current directory to `$LOAD_PATH` only when `--require` option is used (#117).
* Remove manual heartbeat support, see [Long-running message processing section in README](README.md#long-running-message-processing)

## racecar v0.5.0

* Add support for manually sending heartbeats with `heartbeat` (#105).
* Allow configuring `sasl_over_ssl`.

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
