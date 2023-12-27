#!/bin/sh

sed -i "s/^version = .*/version = \"$1\"/" **/Cargo.toml