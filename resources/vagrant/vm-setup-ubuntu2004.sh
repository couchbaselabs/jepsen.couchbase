#!/usr/bin/env bash

echo "Running vm-setup script"

# setting up required packages
echo "Installing required packages"
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt update
apt upgrade
apt install -y ntpdate daemonlogger net-tools

# enabling core dumps to be written to the vagrants shared directory
echo "Setting up core dumps"
echo "Changing core dump location to /tmp"
# Disable apport crash reporter, as it will override the core_pattern otherwise.
systemctl disable apport.service
echo "kernel.core_pattern=/tmp/core.%e.%p.%h.%t" >> /etc/sysctl.conf
sysctl -p
echo "Setting default systemd daemon core limit to Infinity"
echo "DefaultLimitCORE=infinity" >> /etc/systemd/system.conf
# Change root user credentials
echo "root:root" | chpasswd
