# kcr
Kafka Cassette Recorder P.O.C.

## Usage

```
$ kcr --help

Usage: kcr [OPTIONS] COMMAND [ARGS]...

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

### Play (wip)

```
$kcr play --help

Usage: kcr play [OPTIONS]

  Playback a cassette to a Kafka topic.

Options:
  --cassette TEXT        Kafka Cassette Recorder directory for playback
                         (REQUIRED)
  --playback-rate FLOAT  Playback rate in percent
  --topic TEXT           Kafka topic to record (REQUIRED)
  -h, --help             Show this message and exit
```


## Running the program

Create a recording from a simple, local cluster:

```
gradle run --args="record --topic $MY_TOPIC --data-directory data"
```

Create a recording from secure cluster, like Confluent Cloud:

```
gradle run --args="--bootstrap-servers $MY_BOOTSTRAP_SERVERS --security-protocol SASL_PLAIN --sasl-mechanism PLAIN --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic $MY_TOPIC --data-directory data"
```
