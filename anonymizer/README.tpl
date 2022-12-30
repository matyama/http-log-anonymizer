# {{crate}}
![Version: {{version}}](https://img.shields.io/badge/version-{{version}}-blue)

## Environment
The application expects certain set of environment variables (described below). A working
configuration can be found in `../.envrc` can can be automatically loaded with tools such as
[`direnv`](https://direnv.net/).

## Run the pipeline with `cargo`
The basic way how to run the application is as follows:
```bash
cargo run --bin anonymizer --release
```
There are no program argumens, all is done via environment variables.

## Run the pipeline as a `docker-compose` service
The application has been dockerized (see `Dockerfile`) and there is a configuration of a
`docker-compose` service inside `../docker-compose.yml`. It is currently commented out for
development purposes but should work when enabled.

The service depends only on Kafka brokers and the ClickHouse proxy.

## Documentation
The main part of the documentation follows this section. For the full Rust docs run
```bash
cargo doc --open
```

## Notes
 - Note that for development convenience `./http_log.capnp` is a copy of `../http_log.capnp`. It
   makes it easier to make it part of the build script (`build.rs`), especially for `Dockerfile`.
 - The application does not hold any state. The output are either data being inserted into
   ClickHouse or offsets committed back to the Kafka cluster. So it only depends on the persistence
   setup of the services inside `../docker-compose.yml`, which have not been altered (i.e. the
   state will be lost upon restart).

{{readme}}

