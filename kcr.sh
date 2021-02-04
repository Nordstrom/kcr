#!/bin/bash

if [[ -z "${KCR_TRUSTSTORE}" ]] ; then
  KCR_TRUSTSTORE="${HOME}/var/private/truststore.jks"
fi
if [[ -z "${KCR_JAAS_CONF}" ]] ; then
  KCR_JAAS_CONF="${HOME}/var/private/jaas.conf"
fi

#needed to enable monitoring tools (jconsole, visualvm, etc)
OPTS="-Djava.rmi.server.hostname=127.0.0.1"

script_dir() {
  SOURCE="${BASH_SOURCE[0]}"
  # While $SOURCE is a symlink, resolve it
  while [ -h "$SOURCE" ]; do
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$( readlink "$SOURCE" )"
    # If $SOURCE was a relative symlink (no "/" as prefix) then
    # we need to resolve it relative to the symlink base directory
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
  done
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  echo "$DIR"
}

build_dir() {
  BASEDIR=$( script_dir )
  if [[ -d $BASEDIR/.git ]]; then
    echo "$BASEDIR/build/libs/"
  else
    echo "$BASEDIR/"
  fi
}

cli_filepath() {
  for jar in $( build_dir )kcr-*-all.jar; do
    [[ -f "$jar" ]] && echo $jar
    break
  done
}

cli_exists() {
  [[ -z "$( cli_filepath )" ]] && return 1
  return 0
}

if cli_exists; then
  java $OPTS \
   -Djava.security.auth.login.config=$KCR_JAAS_CONF \
   -Djavax.net.ssl.trustStore=$KCR_TRUSTSTORE \
   -Djavax.net.ssl.trustStoreType=JKS \
   -jar $( cli_filepath ) "$@"
else
  { echo -e "\e[01;31mOops!\e[0m kcr couldn't be found at $( build_dir )" >&2; exit 1; }
fi
