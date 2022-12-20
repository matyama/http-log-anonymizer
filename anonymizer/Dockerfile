ARG RUST_TAG=1.66-slim
FROM rust:${RUST_TAG} AS runtime

RUN apt-get update -qq \
	&& apt-get install -qq -y --no-install-recommends \
		capnproto \
		libcapnp-dev \
		make \
		librdkafka1 \
		librdkafka-dev \
		clang \
		cmake \
	&& rm -rf /var/lib/apt/lists/*

ENV RUST_LOG anonymizer=INFO,librdkafka=WARN
ENV NUM_CONSUMERS 2
ENV KAFKA__TOPIC http_log
ENV KAFKA__BROKERS localhost:9092
ENV KAFKA__GROUP_ID anonymizer-group
ENV KAFKA__RETRIES 3
ENV KAFKA__RETRY_DELAY 2000
ENV CH__URL http://localhost:8124
ENV CH__USER default
ENV CH__PASSWORD ""
ENV CH__DATABASE default
ENV CH__TCP_KEEPALIVE 60
ENV CH__TARGET_TABLE http_log
ENV CH__CREATE_TABLE true
ENV CH__MAX_ENTRIES 1048575
ENV CH__RATE_LIMIT 65
ENV CH__RETRIES 3

WORKDIR app

COPY . .

RUN cargo build --release --bins

RUN cp /app/target/release/anonymizer /usr/local/bin/
CMD anonymizer

