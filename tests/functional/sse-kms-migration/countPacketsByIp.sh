#!/bin/bash

set -e -o pipefail

# Needs sudo to run tcpdump

# Usage: sudo ./countPacketsByIp.sh 5696 1000

PORT=${1:-5696}
PACKETS_COUNT=${2:-1000}

# tcpdump options:
# -i lo: listen on the loopback interface (localhost)
# -n: don't resolve hostnames
# -t: don't print a timestamp
# -q: quiet mode, less verbose output
# -c $PACKETS_COUNT: capture exactly $PACKETS_COUNT packets then exit naturally,
#   allowing the pipeline (awk | sort | uniq-c) to drain and output results.
# 'tcp dst port $PORT and (tcp[tcpflags] & tcp-push != 0)': PSH-only filter
#   so pure ACKs are excluded, keeping counts 1:1 with KMIP requests.

# Output of tcpdump will look like this:
# IP 127.0.0.1.33428 > 127.0.0.3.5696: tcp 341

# Print only the destination part
# Remove the port number at the end
# Sort and count unique occurrences and trim spaces

# Output looks like this:
# 332 127.0.0.1
# 323 127.0.0.2
# 345 127.0.0.3

tcpdump -i lo -n -t -q -c $PACKETS_COUNT "tcp dst port ${PORT} and (tcp[tcpflags] & tcp-push != 0)" | \
  awk '{print $4}' | \
  sed 's/\.[^.]*$//' | \
  sort | \
  uniq -c | \
  sed 's/^\s*//'
