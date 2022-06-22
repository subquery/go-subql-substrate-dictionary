FROM ubuntu as builder
RUN apt-get update && apt-get install -y git golang make libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
RUN git clone -b v6.29.3 --depth 1 https://github.com/facebook/rocksdb.git /rocksdb && cd /rocksdb &&\
    export CXXFLAGS='-Wno-error=deprecated-copy -Wno-error=pessimizing-move -Wno-error=class-memaccess' &&\
    make static_lib
COPY ./ /data/src/
RUN go env -w CGO_CFLAGS="-I/rocksdb/include" && \
    go env -w CGO_LDFLAGS="-L/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" &&\
    cd /data/src/src && go build -ldflags="-extldflags=-static"

#FROM ubuntu
#FROM scratch
FROM alpine
COPY --from=builder /data/src/src/src /go-subql-substrate-dictionary
