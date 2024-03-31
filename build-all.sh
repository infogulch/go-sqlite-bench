#!/bin/sh

# run in repo root directory
cd `dirname "$0"`

# build all benchmarks in parent directory

echo build craw     && go build -o ./bench ./cmd/bench-craw
echo build eaton    && go build -o ./bench ./cmd/bench-eaton
echo build mattn    && go build -o ./bench ./cmd/bench-mattn
echo build modernc  && go build -o ./bench ./cmd/bench-modernc
echo build ncruces  && go build -o ./bench ./cmd/bench-ncruces
echo build sqinn    && go build -o ./bench ./cmd/bench-sqinn
echo build zombie   && go build -o ./bench ./cmd/bench-zombie
