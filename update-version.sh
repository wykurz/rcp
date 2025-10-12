#!/bin/sh

sed -i "s/version = \"$1\"/version = \"$2\"/" Cargo.toml flake.nix