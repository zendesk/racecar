FROM cimg/ruby:2.7.2

RUN sudo apt-get update
RUN sudo apt-get install docker

WORKDIR /app
COPY . .

RUN bundle install
