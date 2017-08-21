# Racecar

Introducing Racecar, your friendly and easy-to-approach Kafka consumer framework!

Using [ruby-kafka](https://github.com/zendesk/ruby-kafka) directly can be a challenge: it's a flexible library with lots of knobs and options. Most users don't need that level of flexibility, though. Racecar provides a simple and intuitive way to build and configure Kafka consumers that optionally integrates seemlessly with Rails.

## Table of content

1. [Installation](#installation)
2. [Usage](#usage)
    1. [Creating consumers](#creating-consumers)
    2. [Running consumers](#running-consumers)
    3. [Configuration](#configuration)
    4. [Testing consumers](#testing-consumers)
    5. [Deploying consumers](#deploying-consumers)
    6. [Handling errors](#handling-errors)
    7. [Logging](#logging)
    8. [Operations](#operations)
3. [Development](#development)
4. [Contributing](#contributing)
5. [Copyright and license](#copyright-and-license)


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

Racecar is built for simplicity of development and operation. If you need more flexibility, it's quite straightforward to build your own Kafka consumer executables using [ruby-kafka](https://github.com/zendesk/ruby-kafka#consuming-messages-from-kafka) directly.

First, a short introduction to the Kafka consumer concept as well as some basic background on Kafka.

Kafka stores messages in so-called _partitions_ which are grouped into _topics_. Within a partition, each message gets a unique offset.

In Kafka, _consumer groups_ are sets of processes that collaboratively process messages from one or more Kafka topics; they divide up the topic partitions amongst themselves and make sure to reassign the partitions held by any member of the group that happens to crash or otherwise becomes unavailable, thus minimizing the risk of disruption. A consumer in a group is responsible for keeping track of which messages in a partition it has processed – since messages are processed in-order within a single partition, this means keeping track of the _offset_ into the partition that has been processed. Consumers periodically _commit_ these offsets to the Kafka brokers, making sure that another consumer can resume from those positions if there is a crash.

### Creating consumers

A Racecar consumer is a simple Rails class that inherits from `Racecar::Consumer`:

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

This will create a file at `app/consumers/tap_dance_consumer.rb` which you can modify to your liking. Add one or more calls to  `subscribes_to` in order to have the consumer subscribe to Kafka topics.

Now run your consumer with `bundle exec racecar TapDanceConsumer`.

Note: if you're not using Rails, you'll have to add the file yourself. No-one will judge you for copy-pasting it.

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

If you want to process whole _batches_ of messages at a time, simply rename your `#process` method to `#process_batch`. The method will now be called with a "batch" object rather than a message:

```ruby
class ArchiveEventsConsumer < Racecar::Consumer
  subscribes_to "events"

  def process_batch(batch)
    file_name = [
      batch.topic, # the topic this batch of messages came from.
      batch.partition, # the partition this batch of messages came from.
      batch.first_offset, # offset of the first message in the batch.
      batch.last_offset, # offset of the last message in the batch.
    ].join("-")

    File.open(file_name, "w") do |file|
      # the messages in the batch.
      batch.messages.each do |message|
        file << message.value
      end
    end
  end
end
```

An important detail is that, if an exception is raised while processing a batch, the _whole batch_ is re-processed.

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

### Configuration

Racecar provides a flexible way to configure your consumer in a way that feels at home in a Rails application. If you haven't already, run `bundle exec rails generate racecar:install` in order to generate a config file. You'll get a separate section for each Rails environment, with the common configuration values in a shared `common` section.

**Note:** many of these configuration keys correspond directly to similarly named concepts in [ruby-kafka](https://github.com/zendesk/ruby-kafka); for more details on low-level operations, read that project's documentation.

It's also possible to configure Racecar using environment variables. For any given configuration key, there should be a corresponding environment variable with the prefix `RACECAR_`, in upper case. For instance, in order to configure the client id, set `RACECAR_CLIENT_ID=some-id` in the process in which the Racecar consumer is launched. You can set `brokers` by passing a comma-separated list, e.g. `RACECAR_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092`.

#### Basic configuration

* `brokers` – A list of Kafka brokers in the cluster that you're consuming from. Defaults to `localhost` on port 9092, the default Kafka port.
* `client_id` – A string used to identify the client in logs and metrics.
* `group_id` – The group id to use for a given group of consumers. Note that this _must_ be different for each consumer class. If left blank a group id is generated based on the consumer class name.
* `group_id_prefix` – A prefix used when generating consumer group names. For instance, if you set the prefix to be `kevin.` and your consumer class is named `BaconConsumer`, the resulting consumer group will be named `kevin.bacon_consumer`.

#### Consumer checkpointing

The consumers will checkpoint their positions from time to time in order to be able to recover from failures. This is called _committing offsets_, since it's done by tracking the offset reached in each partition being processed, and committing those offset numbers to the Kafka offset storage API. If you can tolerate more double-processing after a failure, you can increase the interval between commits in order to better performance. You can also do the opposite if you prefer less chance of double-processing.

* `offset_commit_interval` – How often to save the consumer's position in Kafka. Default is every 10 seconds.
* `offset_commit_threshold` – How many messages to process before forcing a checkpoint. Default is 0, which means there's no limit. Setting this to e.g. 100 makes the consumer stop every 100 messages to checkpoint its position.

#### Timeouts & intervals

All timeouts are defined in number of seconds.

* `heartbeat_interval` – How often to send a heartbeat message to Kafka.
* `pause_timeout` – How long to pause a partition for if the consumer raises an exception while processing a message.
* `connect_timeout` – How long to wait when trying to connect to a Kafka broker. Default is 10 seconds.
* `socket_timeout` – How long to wait when trying to communicate with a Kafka broker. Default is 30 seconds.
* `max_wait_time` – How long to allow the Kafka brokers to wait before returning messages. A higher number means larger batches, at the cost of higher latency. Default is 5 seconds.

#### SSL encryption, authentication & authorization

* `ssl_ca_cert` – A valid SSL certificate authority, as a string.
* `ssl_ca_cert_file_path` - The path to a valid SSL certificate authority file.
* `ssl_client_cert` – A valid SSL client certificate, as a string.
* `ssl_client_cert_key` – A valid SSL client certificate key, as a string.

#### SASL encryption, authentication & authorization

Racecar has support for using SASL to authenticate clients using either the GSSAPI or PLAIN mechanism.

If using GSSAPI:

* `sasl_gssapi_principal` – The GSSAPI principal
* `sasl_gssapi_keytab` – Optional GSSAPI keytab.

If using PLAIN:

* `sasl_plain_authzid` – The authorization identity to use.
* `sasl_plain_username` – The username used to authenticate.
* `sasl_plain_password` – The password used to authenticate.


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

If you've ever used Heroku you'll recognize the format – indeed, deploying to Heroku should just work if you add Racecar invocations to your Procfile.

With Foreman, you can easily run these processes locally by executing `foreman run`; in production you'll want to _export_ to another process management format such as Upstart or Runit. [capistrano-foreman](https://github.com/hyperoslo/capistrano-foreman) allows you to do this with Capistrano.


### Handling errors

When processing messages from a Kafka topic, your code may encounter an error and raise an exception. The cause is typically on of two things:

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

Tracking these errors in an exception tracker or some other monitoring system is highly recommended.

If, on the other hand, the exception was cause by a temporary network or database problem, you will probably want to retry processing of the message after some time has passed. By default, if an exception is raised by the `#process` method, the consumer will pause all processing of the message's partition for some number of seconds, configured by setting the `pause_timeout` configuration variable. This allows the consumer to continue processing messages from other partitions that may not be impacted by the problem while still making sure to not drop the original message.

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

It is highly recommended that you set up an error handler.


### Logging

By default, Racecar will log to `STDOUT`. If you're using Rails, your application code will use whatever logger you've configured there.

In order to make Racecar log its own operations to a log file, set the `logfile` configuration variable or pass `--log filename.log` to the `racecar` command.


### Operations

In order to gracefully shut down a Racecar consumer process, send it the `SIGTERM` signal. Most process supervisors such as Runit and Kubernetes send this signal when shutting down a process, so using those systems will make things easier.

In order to introspect the configuration of a consumer process, send it the `SIGUSR1` signal. This will make Racecar print its configuration to the standard error file descriptor associated with the consumer process, so you'll need to know where that is written to.


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/racecar.

## Copyright and license

Copyright 2017 Daniel Schierbeck & Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
