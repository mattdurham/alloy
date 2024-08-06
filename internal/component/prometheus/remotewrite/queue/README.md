# Queue based remote write client

# Caveat: Consider this the most experimental possible

## Overview

The `prometheus.remote.write.queue` goals are to set reliable and repeatable memory and cpu usage based on the number of incoming and outgoing series. There are four broad parts to the system.

1. The `prometheus.remote.write.queue` component itself. This handles the lifecycle of the Alloy system.
2. The `cbor` serializer. This converts an array of series into a serializable format. Included is the ability to specify metadata. [Concise Binary Object Representation (CBOR)](https://en.wikipedia.org/wiki/CBOR) is an IETF format.
3. The `filequeue` is where the CBOR buffers are written to. This has a series of files that are committed to disk and then are read.
4. The `network` handles sending data. The data is sharded by the label hash across any number of loops that send data.

## Design Goals

The initial goal is to get a v1 version that will not include many features found in the existing remote write. This includes TLS specific options, scaling the network up, multiple endpoints. Some of these features will be added over time, some will not.

## Implementation Goals

In normal operation memory should be limited to the scrape, memory waiting to be written to the file queue and memory in the queue to write to the network. This means that memory should not fluctuate based on the number of metrics written to disk and should be consistent.

Replayability, series will be replayed in the event of network downtime, or Alloy restart. Series TTL will be checked on writing to the `filequeue` and on sending to `network`.

Metadata, runing on several settings will allow metadata to be 