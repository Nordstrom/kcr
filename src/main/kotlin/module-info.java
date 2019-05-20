module com.nordstrom.kafka.kcr {

    requires clikt;

    requires jdk.unsupported;

    requires kafka.clients;

    requires kotlin.stdlib;

    requires kotlinx.coroutines.core;
    requires kotlinx.serialization.runtime;

    requires micrometer.core;
    requires micrometer.registry.statsd;

    requires slf4j.api;

}