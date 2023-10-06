FROM cimg/ruby:2.7.8

RUN sudo apt-get update
RUN sudo apt-get install docker

WORKDIR /app
COPY . .

RUN bundle install
