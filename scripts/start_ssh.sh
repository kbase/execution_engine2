#!/bin/env bash
set -e
set -x

echo "kbase:kbase" | /usr/sbin/chpasswd

ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N '' \
&& ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key -N '' \
&& ssh-keygen -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N '' \
&& ssh-keygen -t ed25519 -f /etc/ssh/ssh_host_ed25519_key -N ''

chmod 0600 /etc/ssh/ssh_host_rsa_key
mkdir /home/kbase && chmod 0777 /home/kbase

/usr/sbin/sshd