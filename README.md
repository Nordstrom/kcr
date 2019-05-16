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
  --topic TEXT     Kafka topic to record (REQUIRED)
  -h, --help       Show this message and exit
```


## Running the program

### Recording

Create a recording from a simple, local cluster:

```
gradle run --args="record --topic $MY_TOPIC --data-directory data"
```

Create a recording from secure cluster, like Confluent Cloud:

```
gradle run --args="--bootstrap-servers $MY_BOOTSTRAP_SERVERS --security-protocol SASL_PLAIN --sasl-mechanism PLAIN --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic $MY_TOPIC --data-directory data"
```

### Playback (wip)

Currently only writes record to console log.  Playback is at the capture rate of the cassette (i.e., if you recorded a stream
with 5 message/sec, playback will also be at 5 message/sec)

```
gradle run --args="playback --cassette data/my-topic-yyyymmdd_hhmm
```
