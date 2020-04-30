#!/usr/bin/env bash

echo "Installing deps"
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt update
apt install -y libstdc++6 ntpdate daemonlogger gzip zip unzip