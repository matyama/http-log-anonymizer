ARG RUST_TAG=1.66.0-stable
FROM clux/muslrust:${RUST_TAG} AS chef 
RUN apt-get update -qq \
	&& apt-get install -qq -y --no-install-recommends \
		capnproto \
		libcapnp-dev \
		librdkafka-dev \
		upx-ucl \
	&& rm -rf /var/lib/apt/lists/*
USER root
RUN cargo install cargo-chef 
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build and cache dependencies
RUN cargo chef cook \
	--profile release-min \
	--target x86_64-unknown-linux-musl \
	--recipe-path recipe.json
# Build application
COPY . .
RUN cargo build \
	--profile release-min \
	--target x86_64-unknown-linux-musl \
	--bin anonymizer
# Compress binary
RUN upx --best --lzma \
	/app/target/x86_64-unknown-linux-musl/release-min/anonymizer

# Build runtime image without the Rust toolchain and additional dependencies
FROM alpine:3.17 AS runtime
RUN addgroup -S anonymizer && adduser -S anonymizer -G anonymizer
ENV RUST_LOG anonymizer=INFO,librdkafka=WARN
ENV NUM_CONSUMERS 2
ENV SHUTDOWN_TIMEOUT 5
ENV MPSC_BUFFER_SIZE 1024
ENV MPSC_SEND_TIMEOUT 1000
ENV KAFKA__TOPIC http_log
ENV KAFKA__BROKERS localhost:9092
ENV KAFKA__GROUP_ID anonymizer-group
ENV KAFKA__RETRIES 3
ENV KAFKA__RETRY_DELAY 2000
ENV CH__URL http://localhost:8124
ENV CH__USER default
ENV CH__PASSWORD ""
ENV CH__DATABASE default
ENV CH__TIMEOUT 10
ENV CH__TCP_KEEPALIVE 60
ENV CH__TARGET_TABLE http_log
ENV CH__CREATE_TABLE true
ENV CH__MAX_BLOCK_SIZE 4096
ENV CH__RATE_LIMIT 65
ENV CH__RETRIES 3
ENV TELEMETRY__PROMETHEUS_EXPORTER_PORT 9464
ENV TELEMETRY__LOKI_URL http://localhost:3100
COPY --from=builder \
	/app/target/x86_64-unknown-linux-musl/release-min/anonymizer \
	/usr/local/bin/
USER anonymizer
CMD ["anonymizer"]

