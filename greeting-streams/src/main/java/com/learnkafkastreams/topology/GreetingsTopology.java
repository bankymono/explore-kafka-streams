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
    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS
//                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH
//                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);
        mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

        var modifiedStream =mergedStream
//                .filter((key, value) -> value.length() > 5)
                .mapValues((readOnlyKey,value) -> value.toUpperCase());
//        .flatMap((key, value) -> {
//            var newValues = Arrays.asList(value.split(""));
//            return newValues.stream().map(val -> KeyValue.pair(key, val))
//                    .collect(Collectors.toList());
//        });

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
