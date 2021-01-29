# kcr
Kafka Cassette Recorder

A utility to record and playback messages from any Kafka topic.  This tool can be useful to capture
production data streams for playback in a disaster recovery scenario or for load testing.

Message key and value are stored as ASCII-encoded hexidecimal along with message header parameters
(stored as string key/value).

Messages are stored in a 'cassette' (data directory) by partition.  Playback of a cassette is at the
same relative rate as was captured so message-rate peaks and valleys of the captured message stream is
reconstructed.

Storage format is in json.

## Usage

```
$ kcr --help

Usage: kcr [OPTIONS] COMMAND [ARGS]...

  Apache Kafka topic record/playback tool

Options:
  --bootstrap-servers TEXT  Kafka bootstrap server list
  --security-protocol TEXT  Security protocol
  --sasl-mechanism TEXT     SASL mechanism
  --sasl-username TEXT      SASL username
  --sasl-password TEXT      SASL password
  -h, --help                Show this message and exit

Commands:
  play    Playback a cassette to a Kafka topic.
  record  Record a Kafka topic to a cassette.

v0.1/0.1
```

### Record

```
$ kcr record --help

Usage: kcr record [OPTIONS]

  Record a Kafka topic to a cassette.

Options:
  --data-directory TEXT    Kafka Cassette Recorder data directory for
                           recording (default=kcr)
  --group-id TEXT          Kafka consumer group id (default=kcr-<topic>-gid)
  --topic TEXT             Kafka topic to record (REQUIRED)
  --duration TEXT          Kafka duration for recording, format must be like
                           **h**m**s
  --header-timestamp TEXT  Use timestamp from header parameter ignoring record
                           timestamp
  --consumer-config TEXT   Optional Kafka Consumer configuration file.
                           OVERWRITES any command-line values.
  -h, --help               Show this message and exit
```

### Play

```
$kcr play --help

Usage: kcr play [OPTIONS]

  Playback a cassette to a Kafka topic.

Options:
  --cassette TEXT         Kafka Cassette Recorder directory for playback
                          (REQUIRED)
  --playback-rate FLOAT   Playback rate multiplier (1.0 = play at capture
                          rate, 2.0 = playback at twice capture rate)
  --topic TEXT            Kafka topic to write (REQUIRED)
  --producer-config TEXT  Optional Kafka Producer configuration file.
                          OVERWRITES any command-line values.
  --info                  List information about a Cassette, then exit
  --pause                 Pause at end of playback (ctrl-c to exit)
  --number-of-runs TEXT   Number of times to run the playback
  --duration TEXT         Kafka duration for playback, format must be like
                          **h**m**s
  -h, --help              Show this message and exit
```

## Metrics

Metrics are written as [io.micrometer](https://micrometer.io/docs) Timer and Counter `Meter` primitives using the [`jmx`](https://micrometer.io/docs/registry/jmx) registry.

| Metric | Description|
| :--- | :--- |
|kcr.recorder||
|duration-ms|Overall duration of recording session in milliseconds|
|elapsed-ms|Elapsed time of recording session in milliseconds|
|write.total|Total record writes|
|write.total.partition.nn|Total record writes for partition 'nn'|
|kcr.player||
|duration-ms|Overall duration of playback session in milliseconds|
|duration-ms.partition.nn|Duration of playback for partition 'nn'|
|elapsed-ms|Elapsed time of playback session in milliseconds|
|send.total|Total record sends for playback session|
|send.total.partition.nn|Total record sends for partition 'nn'|


## Running the program

### Build it first

```
gradle clean build
```


### Recording

Create a recording from a simple, local cluster:

```
java -jar ./build/libs/kcr-all.jar record --topic my-topic --data-directory data
```

Create a recording from secure cluster, like Confluent Cloud:

```
java -jar ./build/libs/kcr-all.jar --bootstrap-servers $MY_BOOTSTRAP_SERVERS --security-protocol SASL_PLAIN --sasl-mechanism PLAIN --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic my-topic --data-directory data
```

### Playback (wip)

Playback is at the capture rate of the cassette (i.e., if you recorded a stream with 5 message/sec, playback will also be at 5 message/sec)

```
java -jar ./build/libs/kcr-all.jar play --cassette data/my-topic-yyyymmdd_hhmm --topic my-topic-too
```

### Helper scripts

```
./scripts/kcr-record <TOPIC>

#e.g., ./scripts/kcr-record sea-of-time
```

```
./scripts/kcr-playback <TARGET_TOPIC> <CASSETTE_DIR>

#e.g., ./scripts/kcr-playback sea-of-science ./data/kcr-sea-of-time-20190517-1708
```

# Example

The `./example` directory has a `docker-compose.yml` that will start a local kafka cluster that can be used for testing.  `kcr` record/play commands default to `localhost:9092`.

`create-pepperland` will create several test topics using `kafka-topics.sh` (assumes $CP is defined to point to kafka distribution)


# Roadmap

* Switch to `picocli` for command-line arguments
* Use `avro` for cassette format
* Record / playback from AWS S3
