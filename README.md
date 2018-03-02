# zz-trading-poc

script for query data from different brokers, normalize and inject into kafka topic

# BUILDING

- Build docker image:
  * git clone https://github.com/wjjpt/zz-trading-poc.git
  * cd src/
  * docker build -t wjjpt/stocks2k .

# EXECUTING

- Execute app using docker image:
  * collect API keys for 1forge and quandl
  * docker run --env KAFKA_BROKER=X.X.X.X --env KAFKA_PORT=9092 --env KAFKA_TOPIC='itrading'--env IFORGE_APIKEY='XXX' --env QUANDL_APIKEY='XXX' -ti wjjpt/stocks2k

