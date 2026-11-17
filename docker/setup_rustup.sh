#!/bin/bash

set -eux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/install_rustup.sh
sh /tmp/install_rustup.sh -y --profile minimal
rm /tmp/install_rustup.sh
