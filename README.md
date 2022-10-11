# Racecar

Racecar is a friendly and easy-to-approach Kafka consumer framework. It allows you to write small applications that process messages stored in Kafka topics while optionally integrating with your Rails models.

The framework is based on [rdkafka-ruby](https://github.com/appsignal/rdkafka-ruby), which, when used directly, can be a challenge: it's a flexible library with lots of knobs and options. Most users don't need that level of flexibility, though. Racecar provides a simple and intuitive way to build and configure Kafka consumers.

**NOTE:** Racecar requires Kafka 0.10 or higher.

## Table of content

1. [Installation](#installation)
2. [Usage](#usage)
   1. [Creating consumers](#creating-consumers)
   2. [Running consumers](#running-consumers)
   3. [Producing messages](#producing-messages)
   4. [Configuration](#configuration)
   5. [Testing consumers](#testing-consumers)
   6. [Deploying consumers](#deploying-consumers)
   7. [Handling errors](#handling-errors)
   8. [Logging](#logging)
   9. [Operations](#operations)
   10. [Upgrading from v1 to v2](#upgrading-from-v1-to-v2)
   11. [Compression](#compression)
3. [Development](#development)
4. [Contributing](#contributing)
5. [Support and Discussion](#support-and-discussion)
6. [Copyright and license](#copyright-and-license)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'racecar'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install racecar

Then execute (if you're in a Rails application):

    $ bundle exec rails generate racecar:install

This will add a config file in `config/racecar.yml`.

## Usage

Racecar is built for simplicity of development and operation. First, a short introduction to the Kafka consumer concept as well as some basic background on Kafka.

Kafka stores messages in so-called _partitions_ which are grouped into _topics_. Within a partition, each message gets a unique offset.

In Kafka, _consumer groups_ are sets of processes that collaboratively process messages from one or more Kafka topics; they divide up the topic partitions amongst themselves and make sure to reassign the partitions held by any member of the group that happens to crash or otherwise becomes unavailable, thus minimizing the risk of disruption. A consumer in a group is responsible for keeping track of which messages in a partition it has processed – since messages are processed in-order within a single partition, this means keeping track of the _offset_ into the partition that has been processed. Consumers periodically _commit_ these offsets to the Kafka brokers, making sure that another consumer can resume from those positions if there is a crash.

### Creating consumers

A Racecar consumer is a simple Ruby class that inherits from `Racecar::Consumer`:

```ruby
class UserBanConsumer < Racecar::Consumer
  subscribes_to "user_banned"

  def process(message)
    data = JSON.parse(message.value)
    user = User.find(data["user_id"])
    user.banned = true
    user.save!
  end
end
```

In order to create your own consumer, run the Rails generator `racecar:consumer`:

    $ bundle exec rails generate racecar:consumer TapDance

This will create a file at `app/consumers/tap_dance_consumer.rb` which you can modify to your liking. Add one or more calls to `subscribes_to` in order to have the consumer subscribe to Kafka topics.

Now run your consumer with `bundle exec racecar TapDanceConsumer`.

Note: if you're not using Rails, you'll have to add the file yourself. No-one will judge you for copy-pasting it.

#### Running consumers in parallel (experimental)

Warning - limited battle testing in production environments; use at your own risk!

If you want to process different partitions in parallel, and don't want to deploy a number of instances matching the total partitions of the topic, you can specify the number of workers to spin up - that number of processes will be forked, and each will register its own consumer in the group. Some things to note:

- This would make no difference on a single partitioned topic - only one consumer would ever be assigned a partition. A couple of example configurations to process all partitions in parallel (we'll assume a 15 partition topic):
  - Parallel workers set to 3, 5 separate instances / replicas running in your container orchestrator
  - Parallel workers set to 5, 3 separate instances / replicas running in your container orchestrator
- Since we're forking new processes, the memory demands are a little higher
  - From some initial testing, running 5 parallel workers requires no more than double the memory of running a Racecar consumer without parallelism.

The number of parallel workers is configured per consumer class; you may only want to take advantage of this for busier consumers:

```ruby
class ParallelProcessingConsumer < Racecar::Consumer
  subscribes_to "some-topic"

  self.parallel_workers = 5

  def process(message)
    ...
  end
end
```

#### Initializing consumers

You can optionally add an `initialize` method if you need to do any set-up work before processing messages, e.g.

```ruby
class PushNotificationConsumer < Racecar::Consumer
  subscribes_to "notifications"

  def initialize
    @push_service = PushService.new # pretend this exists.
  end

  def process(message)
    data = JSON.parse(message.value)

    @push_service.notify!(
      recipient: data.fetch("recipient"),
      notification: data.fetch("notification"),
    )
  end
end
```

This is useful to do any one-off work that you wouldn't want to do for each and every message.

#### Setting the starting position

When a consumer is started for the first time, it needs to decide where in each partition to start. By default, it will start at the _beginning_, meaning that all past messages will be processed. If you want to instead start at the _end_ of each partition, change your `subscribes_to` like this:

```ruby
subscribes_to "some-topic", start_from_beginning: false
```

Note that once the consumer has started, it will commit the offsets it has processed until and in the future will resume from those.

#### Processing messages in batches

If you want to process whole _batches_ of messages at a time, simply rename your `#process` method to `#process_batch`. The method will now be called with an array of message objects:

```ruby
class ArchiveEventsConsumer < Racecar::Consumer
  subscribes_to "events"

  def process_batch(messages)
    file_name = [
      messages.first.topic, # the topic this batch of messages came from.
      messages.first.partition, # the partition this batch of messages came from.
      messages.first.offset, # offset of the first message in the batch.
      messages.last.offset, # offset of the last message in the batch.
    ].join("-")

    File.open(file_name, "w") do |file|
      # the messages in the batch.
      messages.each do |message|
        file << message.value
      end
    end
  end
end
```

An important detail is that, if an exception is raised while processing a batch, the _whole batch_ is re-processed.

#### Message headers

Any headers set on the message will be available when consuming the message:

```ruby
message.headers #=> { "Header-A" => 42, ... }
```

#### Long-running message processing

In order to avoid your consumer being kicked out of its group during long-running message processing operations, you'll need to let Kafka regularly know that the consumer is still healthy. There's two mechanisms in place to ensure that:

_Heartbeats:_ They are automatically sent in the background and ensure the broker can still talk to the consumer. This will detect network splits, ungraceful shutdowns, etc.

_Message Fetch Interval:_ Kafka expects the consumer to query for new messages within this time limit. This will detect situations with slow IO or the consumer being stuck in an infinite loop without making actual progress. This limit applies to a whole batch if you do batch processing. Use `max_poll_interval` to increase the default 5 minute timeout, or reduce batching with `fetch_messages`.

#### Tearing down resources when stopping

When a Racecar consumer shuts down, it gets the opportunity to tear down any resources held by the consumer instance. For example, it may make sense to close any open files or network connections. Doing so is simple: just implement a `#teardown` method in your consumer class and it will be called during the shutdown procedure.

```ruby
class ArchiveConsumer < Racecar::Consumer
  subscribes_to "events"

  def initialize
    @file = File.open("archive", "a")
  end

  def process(message)
    @file << message.value
  end

  def teardown
    @file.close
  end
end
```

### Running consumers

Racecar is first and foremost an executable _consumer runner_. The `racecar` executable takes as argument the name of the consumer class that should be run. Racecar automatically loads your Rails application before starting, and you can load any other library you need by passing the `--require` flag, e.g.

    $ bundle exec racecar --require dance_moves TapDanceConsumer

The first time you execute `racecar` with a consumer class a _consumer group_ will be created with a group id derived from the class name (this can be configured). If you start `racecar` with the same consumer class argument multiple times, the processes will join the existing group – even if you start them on other nodes. You will typically want to have at least two consumers in each of your groups – preferably on separate nodes – in order to deal with failures.

### Producing messages

Consumers can produce messages themselves, allowing for powerful stream processing applications that transform and filter message streams. The API for this is simple:

```ruby
class GeoCodingConsumer < Racecar::Consumer
  subscribes_to "pageviews"

  def process(message)
    pageview = JSON.parse(message.value)
    ip_address = pageview.fetch("ip_address")

    country = GeoCode.country(ip_address)

    # Enrich the original message:
    pageview["country"] = country

    # The `produce` method enqueues a message to be delivered after #process
    # returns. It won't actually deliver the message.
    produce(JSON.dump(pageview), topic: "pageviews-with-country", key: pageview["id"])
  end
end
```

The `deliver!` method can be used to block until the broker received all queued published messages (according to the publisher ack settings). This will automatically being called in the shutdown procedure of a consumer.

You can set message headers by passing a `headers:` option with a Hash of headers.

### Configuration

Racecar provides a flexible way to configure your consumer in a way that feels at home in a Rails application. If you haven't already, run `bundle exec rails generate racecar:install` in order to generate a config file. You'll get a separate section for each Rails environment, with the common configuration values in a shared `common` section.

**Note:** many of these configuration keys correspond directly to similarly named concepts in [rdkafka-ruby](https://github.com/appsignal/rdkafka-ruby); for more details on low-level operations, read that project's documentation.

It's also possible to configure Racecar using environment variables. For any given configuration key, there should be a corresponding environment variable with the prefix `RACECAR_`, in upper case. For instance, in order to configure the client id, set `RACECAR_CLIENT_ID=some-id` in the process in which the Racecar consumer is launched. You can set `brokers` by passing a comma-separated list, e.g. `RACECAR_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092`.

Finally, you can configure Racecar directly in Ruby. The file `config/racecar.rb` will be automatically loaded if it exists; in it, you can configure Racecar using a simple API:

```ruby
Racecar.configure do |config|
  # Each config variable can be set using a writer attribute.
  config.brokers = ServiceDiscovery.find("kafka-brokers")
end
```

#### Basic configuration

- `brokers` – A list of Kafka brokers in the cluster that you're consuming from. Defaults to `localhost` on port 9092, the default Kafka port.
- `client_id` – A string used to identify the client in logs and metrics.
- `group_id` – The group id to use for a given group of consumers. Note that this _must_ be different for each consumer class. If left blank a group id is generated based on the consumer class name such that (for example) a consumer with the class name `BaconConsumer` would default to a group id of `bacon-consumer`.
- `group_id_prefix` – A prefix used when generating consumer group names. For instance, if you set the prefix to be `kevin.` and your consumer class is named `BaconConsumer`, the resulting consumer group will be named `kevin.bacon-consumer`.

#### Batches

- `fetch_messages` - The number of messages to fetch in a single batch. This can be set on a per consumer basis.

#### Logging

- `logfile` – A filename that log messages should be written to. Default is `nil`, which means logs will be written to standard output.
- `log_level` – The log level for the Racecar logs, one of `debug`, `info`, `warn`, or `error`. Default is `info`.

#### Consumer checkpointing

The consumers will checkpoint their positions from time to time in order to be able to recover from failures. This is called _committing offsets_, since it's done by tracking the offset reached in each partition being processed, and committing those offset numbers to the Kafka offset storage API. If you can tolerate more double-processing after a failure, you can increase the interval between commits in order to better performance. You can also do the opposite if you prefer less chance of double-processing.

- `offset_commit_interval` – How often to save the consumer's position in Kafka. Default is every 10 seconds.

#### Timeouts & intervals

All timeouts are defined in number of seconds.

- `session_timeout` – The idle timeout after which a consumer is kicked out of the group. Consumers must send heartbeats with at least this frequency.
- `heartbeat_interval` – How often to send a heartbeat message to Kafka.
- `max_poll_interval` – The maximum time between two message fetches before the consumer is kicked out of the group. Put differently, your (batch) processing must finish earlier than this.
- `pause_timeout` – How long to pause a partition for if the consumer raises an exception while processing a message. Default is to pause for 10 seconds. Set this to `0` in order to disable automatic pausing of partitions or to `-1` to pause indefinitely.
- `pause_with_exponential_backoff` – Set to `true` if you want to double the `pause_timeout` on each consecutive failure of a particular partition.
- `socket_timeout` – How long to wait when trying to communicate with a Kafka broker. Default is 30 seconds.
- `max_wait_time` – How long to allow the Kafka brokers to wait before returning messages. A higher number means larger batches, at the cost of higher latency. Default is 1 second.
- `message_timeout` – How long to try to deliver a produced message before finally giving up. Default is 5 minutes. Transient errors are automatically retried. If a message delivery fails, the current read message batch is retried.
- `statistics_interval` – How frequently librdkafka should publish statistics about its consumers and producers; you must also add a `statistics_callback` method to your processor, otherwise the stats are disabled. The default is 1 second, however this can be quite memory hungry, so you may want to tune this and monitor.

#### Memory & network usage

Kafka is _really_ good at throwing data at consumers, so you may want to tune these variables in order to avoid ballooning your process' memory or saturating your network capacity.

Racecar uses [rdkafka-ruby](https://github.com/appsignal/rdkafka-ruby) under the hood, which fetches messages from the Kafka brokers in a background thread. This thread pushes fetch responses, possible containing messages from many partitions, into a queue that is read by the processing thread (AKA your code). The main way to control the fetcher thread is to control the size of those responses and the size of the queue.

- `max_bytes` — Maximum amount of data the broker shall return for a Fetch request.
- `min_message_queue_size` — The minimum number of messages in the local consumer queue.

The memory usage limit is roughly estimated as `max_bytes * min_message_queue_size`, plus whatever your application uses.

#### SSL encryption, authentication & authorization

- `security_protocol` – Protocol used to communicate with brokers (`:ssl`)
- `ssl_ca_location` – File or directory path to CA certificate(s) for verifying the broker's key
- `ssl_crl_location` – Path to CRL for verifying broker's certificate validity
- `ssl_keystore_location` – Path to client's keystore (PKCS#12) used for authentication
- `ssl_keystore_password` – Client's keystore (PKCS#12) password
- `ssl_certificate_location` – Path to the certificate
- `ssl_key_location` – Path to client's certificate used for authentication
- `ssl_key_password` – Client's certificate password

#### SASL encryption, authentication & authorization

Racecar has support for using SASL to authenticate clients using either the GSSAPI or PLAIN mechanism either via plaintext or SSL connection.

- `security_protocol` – Protocol used to communicate with brokers (`:sasl_plaintext` `:sasl_ssl`)
- `sasl_mechanism` – SASL mechanism to use for authentication (`GSSAPI` `PLAIN` `SCRAM-SHA-256` `SCRAM-SHA-512`)

- `sasl_kerberos_principal` – This client's Kerberos principal name
- `sasl_kerberos_kinit_cmd` – Full kerberos kinit command string, `%{config.prop.name}` is replaced by corresponding config object value, `%{broker.name}` returns the broker's hostname
- `sasl_kerberos_keytab` – Path to Kerberos keytab file. Uses system default if not set
- `sasl_kerberos_min_time_before_relogin` – Minimum time in milliseconds between key refresh attempts
- `sasl_username` – SASL username for use with the PLAIN and SASL-SCRAM-.. mechanism
- `sasl_password` – SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism

#### Producing messages

These settings are related to consumers that _produce messages to Kafka_.

- `partitioner` – The strategy used to determine which topic partition a message is written to when Racecar produces a value to Kafka. The codec needs to be one of `consistent`, `consistent_random` `murmur2` `murmur2_random` `fnv1a` `fnv1a_random` either as a Symbol or a String, defaults to `consistent_random`
- `producer_compression_codec` – If defined, Racecar will compress messages before writing them to Kafka. The codec needs to be one of `gzip`, `lz4`, or `snappy`, either as a Symbol or a String.

#### Datadog monitoring

Racecar supports [Datadog](https://www.datadoghq.com/) monitoring integration. If you're running a normal Datadog agent on your host, you just need to set `datadog_enabled` to `true`, as the rest of the settings come with sane defaults.

- `datadog_enabled` – Whether Datadog monitoring is enabled (defaults to `false`).
- `datadog_host` – The host running the Datadog agent.
- `datadog_port` – The port of the Datadog agent.
- `datadog_namespace` – The namespace to use for Datadog metrics.
- `datadog_tags` – Tags that should always be set on Datadog metrics.

Furthermore, there's a [standard Datadog dashboard configution file](https://raw.githubusercontent.com/zendesk/racecar/master/extra/datadog-dashboard.json) that you can import to get started with a Racecar dashboard for all of your consumers.

#### Consumers Without Rails

By default, if Rails is detected, it will be automatically started when the consumer is started. There are cases where you might not want or need Rails. You can pass the `--without-rails` option when starting the consumer and Rails won't be started.

### Testing consumers

Since consumers are merely classes that implement a simple interface, they're dead simple to test.

Here's an example of testing a consumer class using [RSpec](http://rspec.info/) and Rails:

```ruby
# app/consumers/create_contacts_consumer.rb
#
# Creates a Contact whenever an email address is written to the
# `email-addresses` topic.
class CreateContactsConsumer < Racecar::Consumer
  subscribes_to "email-addresses"

  def process(message)
    email = message.value

    Contact.create!(email: email)
  end
end

# spec/consumers/create_contacts_consumer_spec.rb
describe CreateContactsConsumer do
  it "creates a Contact for each email address in the topic" do
    message = double("message", value: "john@example.com")
    consumer = CreateContactsConsumer.new

    consumer.process(message)

    expect(Contact.where(email: "john@example.com")).to exist
  end
end
```

### Deploying consumers

If you're already deploying your Rails application using e.g. [Capistrano](http://capistranorb.com/), all you need to do to run your Racecar consumers in production is to have some _process supervisor_ start the processes and manage them for you.

[Foreman](https://ddollar.github.io/foreman/) is a very straightford tool for interfacing with several process supervisor systems. You define your process types in a Procfile, e.g.

```
racecar-process-payments: bundle exec racecar ProcessPaymentsConsumer
racecar-resize-images: bundle exec racecar ResizeImagesConsumer
```

If you've ever used Heroku you'll recognize the format – indeed, deploying to Heroku should just work if you add Racecar invocations to your Procfile and [enable the Heroku integration](#deploying-to-heroku)

With Foreman, you can easily run these processes locally by executing `foreman run`; in production you'll want to _export_ to another process management format such as Upstart or Runit. [capistrano-foreman](https://github.com/hyperoslo/capistrano-foreman) allows you to do this with Capistrano.

#### Deploying to Kubernetes

If you run your applications in Kubernetes, use the following [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) spec as a starting point:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-racecar-deployment
  labels:
    app: my-racecar
spec:
  replicas: 3 # <-- this will give us three consumers in the group.
  selector:
    matchLabels:
      app: my-racecar
  strategy:
    type: Recreate # <-- this is the important part.
  template:
    metadata:
      labels:
        app: my-racecar
    spec:
      containers:
        - name: my-racecar
          image: my-racecar-image
          command: ["bundle", "exec", "racecar", "MyConsumer"]
          env: # <-- you can configure the consumer using environment variables!
            - name: RACECAR_BROKERS
              value: kafka1,kafka2,kafka3
            - name: RACECAR_OFFSET_COMMIT_INTERVAL
              value: 5
```

The important part is the `strategy.type` value, which tells Kubernetes how to upgrade from one version of your Deployment to another. Many services use so-called _rolling updates_, where some but not all containers are replaced with the new version. This is done so that, if the new version doesn't work, the old version is still there to serve most of the requests. For Kafka consumers, this doesn't work well. The reason is that every time a consumer joins or leaves a group, every other consumer in the group needs to stop and synchronize the list of partitions assigned to each group member. So if the group is updated in a rolling fashion, this synchronization would occur over and over again, causing undesirable double-processing of messages as consumers would start only to be synchronized shortly after.

Instead, the `Recreate` update strategy should be used. It completely tears down the existing containers before starting all of the new containers simultaneously, allowing for a single synchronization stage and a much faster, more stable deployment update.

#### Deploying to Heroku

If you run your applications in Heroku and/or use the Heroku Kafka add-on, you application will be provided with 4 ENV variables that allow connecting to the cluster: `KAFKA_URL`, `KAFKA_TRUSTED_CERT`, `KAFKA_CLIENT_CERT`, and `KAFKA_CLIENT_CERT_KEY`.

Racecar has a built-in helper for configuring your application based on these variables – just add `require "racecar/heroku"` and everything should just work.

Please note aliasing the Heroku Kafka add-on will break this integration. If you have a need to do that, please ask on [the discussion board](https://github.com/zendesk/racecar/discussions).

```ruby
# This takes care of setting up your consumer based on the ENV
# variables provided by Heroku.
require "racecar/heroku"

class SomeConsumer < Racecar::Consumer
  # ...
end
```

#### Running consumers in the background

While it is recommended that you use a process supervisor to manage the Racecar consumer processes, it is possible to _daemonize_ the Racecar processes themselves if that is more to your liking. Note that this support is currently in alpha, as it hasn't been tested extensively in production settings.

In order to daemonize Racecar, simply pass in `--daemonize` when executing the command:

    $ bundle exec racecar --daemonize ResizeImagesConsumer

This will start the consumer process in the background. A file containing the process id (the "pidfile") will be created, with the file name being constructed from the consumer class name. If you want to specify the name of the pidfile yourself, pass in `--pidfile=some-file.pid`.

Since the process is daemonized, you need to know the process id (PID) in order to be able to stop it. Use the `racecarctl` command to do this:

    $ bundle exec racecarctl stop --pidfile=some-file.pid

Again, the recommended approach is to manage the processes using process managers. Only do this if you have to.

### Handling errors

#### When processing messages from a Kafka topic, your code may encounter an error and raise an exception. The cause is typically one of two things:

1. The message being processed is somehow malformed or doesn't conform with the assumptions made by the processing code.
2. You're using some external resource such as a database or a network API that is temporarily unavailable.

In the first case, you'll need to either skip the message or deploy a new version of your consumer that can correctly handle the message that caused the error. In order to skip a message, handle the relevant exception in your `#process` method:

```ruby
def process(message)
  data = JSON.parse(message.value)
  # ...
rescue JSON::ParserError => e
  puts "Failed to process message in #{message.topic}/#{message.partition} at offset #{message.offset}: #{e}"
  # It's probably a good idea to report the exception to an exception tracker service.
end
```

Since the exception is handled by your `#process` method and is no longer raised, Racecar will consider the message successfully processed. Tracking these errors in an exception tracker or some other monitoring system is highly recommended, as you otherwise will have little insight into how many messages are being skipped this way.

If, on the other hand, the exception was cause by a temporary network or database problem, you will probably want to retry processing of the message after some time has passed. By default, if an exception is raised by the `#process` method, the consumer will pause all processing of the message's partition for some number of seconds, configured by setting the `pause_timeout` configuration variable. This allows the consumer to continue processing messages from other partitions that may not be impacted by the problem while still making sure to not drop the original message. Since messages in a single Kafka topic partition _must_ be processed in order, it's not possible to keep processing _other_ messages in that partition.

In addition to retrying the processing of messages, Racecar also allows defining an _error handler_ callback that is invoked whenever an exception is raised by your `#process` method. This allows you to track and report errors to a monitoring system:

```ruby
Racecar.config.on_error do |exception, info|
  MyErrorTracker.report(exception, {
    topic: info[:topic],
    partition: info[:partition],
    offset: info[:offset],
  })
end
```

It is highly recommended that you set up an error handler. Please note that the `info` object contains different keys and values depending on whether you are using `process` or `process_batch`. See the `instrumentation_payload` object in the `process` and `process_batch` methods in the `Runner` class for the complete list.

#### Errors related to Compression

A sample error might seem like this:

```
E, [2022-10-09T11:28:29.976548 #15] ERROR -- : (try 5/10): Error for topic subscription #<struct Racecar::Consumer::Subscription topic="support.entity_incremental.views.view_ticket_ids", start_from_beginning=false, max_bytes_per_partition=104857, additional_config={}>: Local: Not implemented (not_implemented)
```

Please see [Compression](#compression)

### Logging

By default, Racecar will log to `STDOUT`. If you're using Rails, your application code will use whatever logger you've configured there.

In order to make Racecar log its own operations to a log file, set the `logfile` configuration variable or pass `--log filename.log` to the `racecar` command.

### Operations

In order to gracefully shut down a Racecar consumer process, send it the `SIGTERM` signal. Most process supervisors such as Runit and Kubernetes send this signal when shutting down a process, so using those systems will make things easier.

In order to introspect the configuration of a consumer process, send it the `SIGUSR1` signal. This will make Racecar print its configuration to the standard error file descriptor associated with the consumer process, so you'll need to know where that is written to.

### Upgrading from v1 to v2

In order to safely upgrade from Racecar v1 to v2, you need to completely shut down your consumer group before starting it up again with the v2 Racecar dependency. In general, you should avoid rolling deploys for consumers groups, so it is likely the case that this will just work for you, but it's a good idea to check first. 

### Compression

Racecar v2 requires a C library (zlib) to compress the messages before producing to the topic. If not already installed on you consumer docker container, please install using follwoing command in Dockerfile of consumer

``` 
apt-get update && apt-get install -y libzstd-dev
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rspec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

The integration tests run against a Kafka instance that is not automatically started from within `rspec`. You can set one up using the provided `docker-compose.yml` by running `docker-compose up`.

### Running RSpec within Docker

There can be behavioural inconsistencies between running the specs on your machine, and in the CI pipeline. Due to this, there is now a Dockerfile included in the project, which is based on the CircleCI ruby 2.7.2 image. This could easily be extended with more Dockerfiles to cover different Ruby versions if desired. In order to run the specs via Docker:

- Uncomment the `tests` service from the docker-compose.yml
- Bring up the stack with `docker-compose up -d`
- Execute the entire suite with `docker-compose run --rm tests rspec`
- Execute a single spec or directory with `docker-compose run --rm tests rspec spec/integration/consumer_spec.rb`

Please note - your code directory is mounted as a volume, so you can make code changes without needing to rebuild

## Contributing

Bug reports and pull requests are welcome on [GitHub](https://github.com/zendesk/racecar). Feel free to [join our Slack team](https://ruby-kafka-slack.herokuapp.com/) and ask how best to contribute!

## Support and Discussion

If you've discovered a bug, please file a [Github issue](https://github.com/zendesk/racecar/issues/new), and make sure to include all the relevant information, including the version of Racecar, rdkafka-ruby, and Kafka that you're using.

If you have other questions, or would like to discuss best practises, or how to contribute to the project, [join our Slack team](https://ruby-kafka-slack.herokuapp.com/)!

## Copyright and license

Copyright 2017 Daniel Schierbeck & Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
