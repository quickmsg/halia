# TODO
# Compile
FROM rustlang/rust:nightly-alpine3.20 AS compiler

WORKDIR /

COPY    . .
RUN     cargo build --release

# Run
FROM    alpine:3.20

# add meilisearch and meilitool to the `/bin` so you can run it from anywhere
# and it's easy to find.
COPY    --from=compiler /target/release/server /bin/server
# to move our PWD in there.
# We don't want to put the meilisearch binary
WORKDIR /storage
RUN touch /storage/data

EXPOSE  3000/tcp
RUN chmod +x /bin/server
ENTRYPOINT ["/bin/server"]