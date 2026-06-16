#!/bin/bash

cargo build --release # build the higgins-tests binaries

./target/release/higgins-tests --port 4932  multi-produce --count=100..10000,100
