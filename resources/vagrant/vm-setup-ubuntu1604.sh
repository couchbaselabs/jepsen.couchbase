#!/usr/bin/env bash

echo "Running vm-setup script"

# setting up required packages
echo "Installing libstdc++6"
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt-get update
apt-get install -y libstdc++6 ntpdate

# enabling core dumps to be written to the vagrants shared directory
echo "Setting up core dumps"
echo "Changing core dump location to vagrant shared folder"
echo "/vagrant/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern
echo "kernel.core_pattern=/vagrant/core.%e.%p.%h.%t" >> /etc/sysctl.conf
sysctl -p
echo "*               soft    core            unlimited" >> /etc/security/limits.conf
echo "*               hard    core            unlimited" >> /etc/security/limits.conf
echo "root               soft    core            unlimited" >> /etc/security/limits.conf
echo "root               hard    core            unlimited" >> /etc/security/limits.conf