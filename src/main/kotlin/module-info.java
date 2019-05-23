module com.nordstrom.kafka.kcr {

    //command-line args parser
    requires clikt;

    requires jdk.unsupported;

    //kafka
    requires kafka.clients;

    requires kotlin.stdlib;

    //kotlin coroutines and serialization
    requires kotlinx.coroutines.core;
    requires kotlinx.serialization.runtime;

    //metrics
    requires micrometer.core;
    requires micrometer.registry.jmx;
    requires micrometer.registry.statsd;

    //logging
    requires slf4j.api;

    //aws
    requires software.amazon.awssdk.services.s3;

}