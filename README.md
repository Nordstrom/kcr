# kcr
Kafka Cassette Recorder

A utility to record and playback messages from any Kafka topic.  This tool can be useful to capture
production data streams for playback in a disaster recovery scenario or for load testing.

Message key and value are stored as ASCII-encoded hexadecimal along with message header parameters
(stored as string key/value).

Messages are stored in a 'cassette' (data directory) by partition.  Playback of a cassette is at the
same relative rate as was captured so message-rate peaks and valleys of the captured message stream is
reconstructed.

Storage format is in json.

## Usage

```
$ ./kcr.sh help

Usage: kcr [-hV] [--bootstrap-servers=<bootstrapServers>] [--sasl-mechanism=<saslMechanism>]
           [--sasl-password=<saslPassword>] [--sasl-username=<saslUsername>] [--security-protocol=<securityProtocol>]
           [COMMAND]
      --bootstrap-servers=<bootstrapServers>
                  Kafka bootstrap server list; default: localhost:9092
  -h, --help      Show this help message and exit.
      --sasl-mechanism=<saslMechanism>
                  SASL mechanism; default: SCRAM-SHA-512
      --sasl-password=<saslPassword>
                  SASL password
      --sasl-username=<saslUsername>
                  SASL username
      --security-protocol=<securityProtocol>
                  Security protocol; default: PLAINTEXT
  -V, --version   Print version information and exit.
Commands:
  play    Playback a cassette to a Kafka topic.
  record  Record a Kafka topic to a cassette.
  help    Displays help information about the specified command
```

### Record

```
$ ./kcr.sh record help

Usage: kcr record [-hV] [--consumer-config=<properties>]
                  [--data-directory=<dataDirectory>]
                  [--duration=<durationValue>] [--group-id=<groupId>]
                  [--timestamp-header-name=<timestampHeaderName>]
                  --topic=<topic>
Record a Kafka topic to a cassette.
      --consumer-config=<properties>
                             Optional Kafka Consumer configuration file.
                               OVERWRITES any command-line values.
      --data-directory=<dataDirectory>
                             Kafka Cassette Recorder data directory for
                               recording (default=kcr)
      --duration=<durationValue>
                             Duration for playback; format must be like
                               **h**m**s
      --group-id=<groupId>   Kafka consumer group id (default=kcr-<topic>-gid)
  -h, --help                 Show this help message and exit.
      --timestamp-header-name=<timestampHeaderName>
                             Kafka message header parameter to extract and use
                               as the record timestamp in epoch format,
                               ignoring record timestamp
      --topic=<topic>        Kafka topic to record
  -V, --version              Print version information and exit.
```

### Play

```
$./kcr play help

Usage: kcr play [-hV] [--info] [--pause] --cassette=<cassette>
                [--duration=<durationValue>]
                [--number-of-plays=<numberOfPlaysValue>]
                [--playback-rate=<playbackRate>]
                [--producer-config=<properties>] --topic=<topic>
Playback a cassette to a Kafka topic.
      --cassette=<cassette>
                        Kafka Cassette Recorder cassette directory for playback
      --duration=<durationValue>
                        Duration for playback; format must be like **h**m**s
  -h, --help            Show this help message and exit.
      --info            List information about a cassette, then exit
                          (default=false)
      --number-of-plays=<numberOfPlaysValue>
                        Number of times to play the cassette
      --pause           Pause at end of playback; ctrl-c to exit (default=false)
      --playback-rate=<playbackRate>
                        Playback rate multiplier (0 = playback as fast as
                          possible, 1.0 = play at capture rate, 2.0 = playback
                          at twice capture rate, default=1.0)
      --producer-config=<properties>
                        Optional Kafka Producer configuration file. OVERWRITES
                          any command-line values.
      --topic=<topic>   Kafka topic to write.
  -V, --version         Print version information and exit.
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
./kcr.sh record --topic my-topic --data-directory data
```

Create a recording from secure cluster, like Confluent Cloud:

```
./kcr.sh --bootstrap-servers $MY_BOOTSTRAP_SERVERS --security-protocol SASL_PLAIN --sasl-mechanism PLAIN --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic my-topic --data-directory data
```

### Playback (wip)

Playback is at the capture rate of the cassette (i.e., if you recorded a stream with 5 message/sec, playback will also be at 5 message/sec)

```
./kcr.sh play --cassette data/my-topic-yyyymmdd_hhmm --topic my-topic-too
```

# Example

The `./example` directory has a `docker-compose.yml` that will start a local kafka cluster that can be used for testing.  `kcr` record/play commands default to `localhost:9092`.

`create-pepperland` will create several test topics using `kafka-topics.sh` (assumes $CP is defined to point to kafka distribution)


# Roadmap

* Record / playback from AWS S3
* Use `avro` for cassette format
