FROM rust:1.70 as builder
WORKDIR /usr/src/service

RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y protobuf-compiler

ARG CARGO_REGISTRIES_WX_INDEX="https://gitlab.waves.exchange/we-private/alexandrie.git"
ARG CARGO_NET_GIT_FETCH_WITH_CLI="true"
ARG CARGO_REGISTRY_AUTH=""
RUN git config --global credential.helper store
RUN echo "https://${CARGO_REGISTRY_AUTH}@gitlab.waves.exchange" > ~/.git-credentials

RUN echo "fn main() {}" > dummy.rs
COPY Cargo.* ./
RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
RUN cargo build -j4 --release
RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
COPY ./src ./src
COPY ./migrations ./migrations

RUN cargo install --path .

RUN cargo install --root /usr/local/cargo diesel_cli --no-default-features --features postgres

FROM debian:11
WORKDIR /usr/www/app
RUN apt-get update && apt-get install -y curl openssl libssl-dev libpq-dev postgresql-client
# RUN curl -ks 'https://cert.host.server/ssl_certs/EnterpriseRootCA.crt' -o '/usr/local/share/ca-certificates/EnterpriseRootCA.crt'
RUN /usr/sbin/update-ca-certificates
COPY --from=builder /usr/local/cargo/bin/service .

COPY --from=builder /usr/local/cargo/bin/diesel .
COPY --from=builder /usr/src/service/migrations ./migrations/

CMD ["./service"]
