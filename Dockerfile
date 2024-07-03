FROM rustlang/rust:nightly AS compiler
RUN apt-get install musl-tools

WORKDIR /

COPY    . .
RUN     rustup target add x86_64-unknown-linux-musl
RUN     cargo build --release --target=x86_64-unknown-linux-musl

FROM    alpine:3.20

COPY    --from=compiler /target/release/server /bin/server
WORKDIR /storage
RUN touch /storage/data
RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --target x86_64-unknown-linux-gnu

EXPOSE  3000/tcp
RUN chmod +x /bin/server
ENTRYPOINT ["/bin/server"]