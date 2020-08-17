# timesink
GRPC service that takes in a future event as:
```
# Event uuid
string id
# Unix timestamp in seconds for the future delivery date
int64 delivery_timestamp
# arbitrary payload, could be JSON or other encoded data
bytes payload 
```

The service will store the event into RocksDB as events are queued for future delivery.
There's a reader that is continuously reading events during real time as now() crosses delivery timestamp.

Right now, the reader is dumb and just counts and prints it once in a while to the log.

There's also a cleaner that follows after the reader, keeping a safe 10 mins time interval behind, and cleans up old events.

How to deploy and scale this:
- Build a Kafka consumer that read in batches from a partition and dumps the future events into RocksDB
- Build a Kafka producer that read events in real time as now() crosses the delivery timestamp
- Periodically checkpoint the offset of the kafka consumer and producer every few seconds into Etcdb
- On restart, re-initialize reader at the right offset

For availability of the RocksDB database, provision replicated Kubernetes storage such as Linstor or StorageOS.
Scale up by running multiple instances of this service, one for each kafka partition.
Message delivery is at least once.

Performance benchmark:
Reader can do about 40k/s on a low-end GCP instance with 2 vCPU cores and SSD storage
Cleaner can go much faster because it uses write batches
Service RPC performs similar to reader, can be much faster if reading from Kafka in batches and use write batch to persist

To build and run this:
```
go build
./timesink
```
Rocksdb is stored at /tmp/timesinkdb/
Clean this directory if you want to wipe all queued events
