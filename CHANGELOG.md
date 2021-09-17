# Changelog

## Unreleased

* Update librdkafka version from 1.4.0 to 1.5.0 by upgrading from rdkafka 0.8.0 to 0.10.0. ([#263](https://github.com/zendesk/racecar/pull/263))

## racecar v2.3.1

* Handle `ERR_NOT_COORDINATOR` (#209)

## racecar v2.3.0

* Add native support for Heroku (#248)
* [Racecar::Consumer] When messages fail to deliver, an extended error with hints is now raised. Instead of `Rdkafka::RdkafkaError` you'll get a `Racecar::MessageDeliveryError` instead. ([#219](https://github.com/zendesk/racecar/pull/219)). If you have set a `Racecar.config.error_handler`, it might need to be updated.
* [Racecar::Consumer] When message delivery times out, Racecar will reset the producer in an attempt to fix some of the potential causes for this error. ([#219](https://github.com/zendesk/racecar/pull/219))
* Validate the `process` and `process_batch` method signature on consumer classes when initializing (#236)
* Add Ruby 3.0 compatibility (#237)
* Introduce parallel runner, which forks a number of independent consumers, allowing partitions to be processed in parallel. ([#222](https://github.com/zendesk/racecar/pull/222))
* [Racecar::Runner] Ensure producer is closed, whether it closes or errors. ([#222](https://github.com/zendesk/racecar/pull/222))
* Configure `statistics_interval` directly in the config. Disable statistics when no callback is defined ([#232](https://github.com/zendesk/racecar/pull/232))

## racecar v2.2.0

* [Racecar::ConsumerSet] **breaking change** `Racecar::ConsumerSet`'s functions `poll` and `batch_pall` expect the max wait values to be given in milliseconds. The defaults were using `config.max_wait_time`, which is in seconds. If you do not directly use `Racecar::ConsumerSet`, or always call its `poll` and `batch_poll` functions by specfiying the max wait time (the first argument), then this breaking change does not affect you. ([#214](https://github.com/zendesk/racecar/pull/214))

## racecar v2.1.1

* [Bugfix] Close RdKafka consumer in ConsumerSet#reset_current_consumer to prevent memory leak (#196)
* [Bugfix] `poll`/`batch_poll` would not retry in edge cases and raise immediately. They still honor the `max_wait_time` setting, but might return no messages instead and only retry on their next call. ([#177](https://github.com/zendesk/racecar/pull/177))

## racecar v2.1.0

* Bump rdkafka to 0.8.0 (#191)

## racecar v2.0.0

* Replace `ruby-kafka` with `rdkafka-ruby` as the low-level library underneath Racecar (#91).
* Fix `max_wait_time` usage (#179).
* Removed config option `sasl_over_ssl`.
* [Racecar::Consumer] Do not pause consuming partitions on exception.
* [Racecar::Consumer] `topic`, `payload` and `key` are mandadory to method `produce`.
* [Racecar::Consumer] `process_batch` retrieves an array of messages instead of batch object.
* [Racecar::Consumer] Remove `offset_retention_time`.
* [Racecar::Consumer] Allow providing `additional_config` for subscriptions.
* [Racecar::Consumer] Provide access to `producer` and `consumer`.
* [Racecar::Consumer] Enforce delivering messages with method `deliver!`.
* [Racecar::Consumer] instead of raising when a partition EOF is reached, the result can be queried through `consumer.last_poll_read_partition_eof?`.
* [Racecar::Config] Remove `offset_retention_time`, `connect_timeout` and `offset_commit_threshold`.
* [Racecar::Config] Pass config to `rdkafka-ruby` via `producer` and `consumer`.
* [Racecar::Config] Replace `max_fetch_queue_size` with `min_message_queue_size`.
* [Racecar::Config] Add `synchronous_commits` to control blocking of `consumer.commit` (default `false`).
* [Racecar::Config] Add `security_protocol` to control protocol between client and broker.
* [Racecar::Config] SSL configuration via `ssl_ca_location`, `ssl_crl_location`, `ssl_keystore_location` and `ssl_keystore_password`.
* [Racecar::Config] SASL configuration via `sasl_mechanism`, `sasl_kerberos_service_name`, `sasl_kerberos_principal`, `sasl_kerberos_kinit_cmd`, `sasl_kerberos_keytab`, `sasl_kerberos_min_time_before_relogin`, `sasl_username` and `sasl_password`.
* [Instrumentation] `produce_message.racecar` sent whenever a produced message is queued. Payload includes `topic`, `key`, `value` and `create_time`.
* [Instrumentation] `acknowledged_message.racecar` send whenever a produced message was successfully received by Kafka. Payload includes `offset` and `partition`, but no message details.
* [Instrumentation] `rdkafka-ruby` does not yet provide instrumentation [rdkafka-ruby#54](https://github.com/appsignal/rdkafka-ruby/issues/54).
* [Instrumentation] if processors define a `statistics_callback`, it will be called once every second for every subscription or producer connection. The first argument will be a Hash, for contents see [librdkafka STATISTICS.md](https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md).
* Add current directory to `$LOAD_PATH` only when `--require` option is used (#117).
* Remove manual heartbeat support, see [Long-running message processing section in README](README.md#long-running-message-processing).
* Rescue exceptions--then log and pass to `on_error`--at the outermost level of `exe/racecar`, so that exceptions raised outside `Cli.run` are not silently discarded (#186).
* When exceptions with a `cause` are logged, recursively log the `cause` detail, separated by `--- Caused by: ---\n`.

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
