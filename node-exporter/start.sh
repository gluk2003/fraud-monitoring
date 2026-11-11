#!/bin/sh
set -eu
# /proc/net points to the reader's network namespace. Read PID 1 explicitly
# to observe the Linux host while exporter remains in the shared Compose network.
mkdir -p /tmp/node-textfile
(
  while :; do
    awk 'BEGIN {
      print "# HELP node_host_network_receive_bytes_total Linux host received bytes"
      print "# TYPE node_host_network_receive_bytes_total counter"
      print "# HELP node_host_network_transmit_bytes_total Linux host transmitted bytes"
      print "# TYPE node_host_network_transmit_bytes_total counter"
    }
    NR > 2 {
      gsub(/:/, "", $1)
      printf "node_host_network_receive_bytes_total{device=\"%s\"} %s\n", $1, $2
      printf "node_host_network_transmit_bytes_total{device=\"%s\"} %s\n", $1, $10
    }' /host/proc/1/net/dev > /tmp/node-textfile/host-network.prom.tmp
    mv /tmp/node-textfile/host-network.prom.tmp /tmp/node-textfile/host-network.prom
    sleep 1
  done
) &
exec /bin/node_exporter --collector.textfile.directory=/tmp/node-textfile "$@"
