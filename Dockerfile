FROM yesq/rocksdb_builder:v6.29.3 as builder
COPY ./ /data/src/
RUN go env -w CGO_CFLAGS="-I/rocksdb/include" && \
    go env -w CGO_LDFLAGS="-L/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" &&\
    cd /data/src/src && go build -ldflags="-extldflags=-static"

#FROM ubuntu
#FROM scratch
FROM alpine
COPY --from=builder /data/src/entrypoint.sh /
COPY --from=builder /data/src/src/src /go-subql-substrate-dictionary
COPY --from=builder /data/src/config.json /data/src/network/* /
COPY --from=builder /data/src/network/*.json /
ENTRYPOINT /entrypoint.sh