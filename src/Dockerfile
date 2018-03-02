FROM alpine:3.7 as builder
MAINTAINER Juan J. Prieto <jjptapia@gmail.com>


ENV BUILD_PACKAGES bash curl-dev ruby-dev build-base
ENV RUBY_PACKAGES ruby ruby-bundler

RUN apk add --no-cache $BUILD_PACKAGES $RUBY_PACKAGES && \
    mkdir /usr/app 

WORKDIR /usr/app

COPY Gemfile /usr/app/
COPY Gemfile.lock /usr/app/
RUN bundle install

FROM alpine:3.7

RUN apk add --no-cache ruby

COPY --from=builder /usr/lib/ruby /usr/lib/ruby
COPY ./stocks2k.rb /usr/local/bin
ENTRYPOINT ["/usr/local/bin/stocks2k.rb"]
