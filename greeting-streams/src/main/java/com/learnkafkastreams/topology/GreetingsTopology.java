package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

public class GreetingsTopology {
    public static String GREETINGS = "greetings";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetinStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        greetinStream.print(Printed.<String, String>toSysOut().withLabel("greetingStream"));

        var modifiedStream =greetinStream
//                .filter((key, value) -> value.length() > 5)
//                .mapValues((readOnlyKey,value) -> value.toUpperCase());
        .flatMap((key, value) -> {
            var newValues = Arrays.asList(value.split(""));
            return newValues.stream().map(val -> KeyValue.pair(key, val))
                    .collect(Collectors.toList());
        });

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
