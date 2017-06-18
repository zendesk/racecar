# Racecar

Introducing Racecar, your friendly and easy-to-approach Kafka consumer framework!

Using [ruby-kafka](https://github.com/zendesk/ruby-kafka) directly can be a challenge: it's a flexible library with lots of knobs and options. Most users don't need that level of flexibility, though. Racecar provides a simple and intuitive way to build and configure Kafka consumers within a Rails application.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'racecar'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install racecar

## Usage

Racecar is built for simplicity of development and operation. If you need more flexibility, it's quite straightforward to build your own Kafka consumer executables using [ruby-kafka](https://github.com/zendesk/ruby-kafka#consuming-messages-from-kafka) directly.

### Creating consumers

Add a file in e.g. `app/consumers/user_ban_consumer.rb`:

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

Now run your consumer with `bundle exec racecar UserBanConsumer`.

You can optionally add an `initialize` method if you need to do any set-up, e.g.

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

### Running consumers

Racecar is first and foremost an executable consumer _runner_. The `racecar` consumer takes as argument the name of the consumer class that should be run. Racecar automatically loads your Rails application before starting, and you can load any other library you need by passing the `--require x` flag, e.g.

    bundle exec racecar --require dance_moves TapDanceConsumer

### Configuration

Racecar provides a flexible way to configure your consumer in a way that feels at home in a Rails application. If you haven't already, run `bundle exec rails generate racecar:install` in order to generate a config file. You'll get a separate section for each Rails environment, with the common configuration values in a shared `common` section.

The possible configuration keys are:

* `brokers` (_required_) – A list of Kafka brokers in the cluster that you're consuming from.
* `client_id` (_optional_) – A string used to identify the client in logs and metrics.
* `group_id_prefix` (_optional_) – A prefix used when generating consumer group names. For instance, if you set the prefix to be `kevin.` and your consumer class is named `BaconConsumer`, the resulting consumer group will be named `kevin.bacon_consumer`.
* `offset_commit_interval` (_optional_) – How often to save the consumer's position in Kafka.
* `heartbeat_interval` (_optional_) – How often to send a heartbeat message to Kafka.
* `pause_timeout` (_optional_) – How long to pause a partition for if the consumer raises an exception while processing a message.
* `connect_timeout` (_optional_) – How long to wait when trying to connect to a Kafka broker.
* `socket_timeout` (_optional_) – How long to wait when trying to communicate with a Kafka broker.

Note that many of these configuration keys correspond directly with similarly named concepts in [ruby-kafka](https://github.com/zendesk/ruby-kafka) for more details on low-level operations, read that project's documentation.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/racecar.

## Copyright and license

Copyright 2015 Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
