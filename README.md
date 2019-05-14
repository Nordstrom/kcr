# kcr
Kafka Cassette Recorder P.O.C.

## Usage

```
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

## Running the program

```
gradle run --args="--bootstrap-servers $MY_BOOTSTRAP_SERVERS --sasl-username $MY_SASL_USERNAME --sasl-password $MY_SASL_PASSWORD record --topic $MY_TOPIC --data-directory data"
```

