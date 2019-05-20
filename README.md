# kcr
Kafka Cassette Recorder P.O.C.

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
  --data-directory TEXT  Kafka Cassette Recorder data directory for recording
                         (default=./data)
  --group-id TEXT        Kafka consumer group id (default=kcr-<topic>-gid
  --topic TEXT           Kafka topic to record (REQUIRED)
  -h, --help             Show this message and exit
```

### Play

```
$kcr play --help

Usage: kcr play [OPTIONS]

  Playback a cassette to a Kafka topic.

Options:
  --cassette TEXT  Kafka Cassette Recorder directory for playback (REQUIRED)
  --topic TEXT     Kafka topic to write (REQUIRED)
  -h, --help       Show this message and exit
```

## Metrics

### Record

| Metric | Description|
| :--- | :--- |
|RECORD||
|kcr.recorder.duration-ms|Overall duration of recording session|
|kcr.recorder.partition.duration-ms,partition=n|Duration of recording for partition 'n'|
|kcr.recorder.partition.write-total,partition=n|Total record writes for partition 'n'|
|PLAYBACK||
|kcr.player.duration-ms|Overall duration of playback session|
|kcr.player.partition.duration-ms,partition=n|Duration of playback for partition 'n'|
|kcr.player.partition.send-total,partition=n|Total record sends for partition 'n'|


## Running the program

### Build it first

```
gradle clean build
```


### Recording

Create a recording from a simple, local cluster:

```
java -jar ./build/libs/kcr.jar record --topic $MY_TOPIC --data-directory data
```

Create a recording from secure cluster, like Confluent Cloud:

```
java -jar ./build/libs/kcr.jar --bootstrap-servers $MY_BOOTSTRAP_SERVERS --security-protocol SASL_PLAIN --sasl-mechanism PLAIN --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic $MY_TOPIC --data-directory data
```

### Playback (wip)

Playback is at the capture rate of the cassette (i.e., if you recorded a stream with 5 message/sec, playback will also be at 5 message/sec)

```
java -jar playback --cassette data/my-topic-yyyymmdd_hhmm
```

### Helper scripts

```
./scripts/topic-record <TOPIC>

#e.g., ./scripts/topic-record sea-of-time
```

```
./scripts/topic-playback <TARGET_TOPIC> <CASSETTE_DIR>

#e.g., ./scripts/topic-playback sea-of-science ./data/kcr-sea-of-time-20190517-1708
```

# Testing

The `./testing` directory has a `docker-compose.yml` that will start a local kafka cluster that can be used for testing.  `kcr` record/playback default to `localhost:9092`.

`create-pepperland` will create several test topics using `kafka-topics.sh` (assumes $CP is defined to point to kafka distribution)

