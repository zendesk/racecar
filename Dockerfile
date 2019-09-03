FROM ruby:2.6
WORKDIR /usr/src/app

COPY Gemfile racecar.gemspec ./
COPY lib/racecar/version.rb ./lib/racecar/version.rb

RUN bundle install

COPY . ./

RUN bundle install

CMD bundle exec racecar --require examples/test_consumer TestConsumer
