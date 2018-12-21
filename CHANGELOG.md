# Changelog

## Unreleased

* Replace `ruby-kafka` with `rdkafka-ruby`
* [Racecar::Consumer] Do not pause consuming partitions on exception
* [Racecar::Consumer] `topic`, `payload` and `key` are mandadory to method `produce`
* [Racecar::Consumer] `process_batch` retrieves an array of messages instead of batch object
* [Racecar::Consumer] Remove `offset_retention_time`
* [Racecar::Consumer] Allow providing `additional_config` for subscriptions
* [Racecar::Consumer] Provide access to `producer` and `consumer`
* [Racecar::Consumer] Enforce delivering messages with method `deliver!`
* [Racecar::Config] Remove `offset_retention_time`, `connect_timeout` and `offset_commit_threshold`
* [Racecar::Config] Pass config to `rdkafka-ruby` via `producer` and `consumer`
* [Racecar::Config] Replace `max_fetch_queue_size` with `min_message_queue_size`
* [Racecar::Config] Add `synchronous_commits` to control blocking of `consumer.commit` (default `false`)
* [Racecar::Config] Add `raise_on_partition_eof` to raise a `Rdkafka::RdkafkaError` when reaching the current end of the partition. Will only be re-raised after new messages arrive. (default `false`)
* [Racecar::Config] Add `security_protocol` to control protocol between client and broker
* [Racecar::Config] SSL configuration via `ssl_ca_location`, `ssl_crl_location`, `ssl_keystore_location` and `ssl_keystore_password`
* [Racecar::Config] SASL configuration via `sasl_mechanism`, `sasl_kerberos_service_name`, `sasl_kerberos_principal`, `sasl_kerberos_kinit_cmd`, `sasl_kerberos_keytab`, `sasl_kerberos_min_time_before_relogin`, `sasl_username` and `sasl_password`
* [Instrumentation] `rdkafka-ruby` does not yet provide instrumentation [rdkafka-ruby#54](https://github.com/appsignal/rdkafka-ruby/issues/54)

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
