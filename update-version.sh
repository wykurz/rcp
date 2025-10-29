#!/bin/sh
git push rad main
sed -i "s/version = \"$1\"/version = \"$2\"/" Cargo.toml flake.nix