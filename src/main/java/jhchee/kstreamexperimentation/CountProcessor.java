package jhchee.kstreamexperimentation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class CountProcessor {

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()));

        // collect when time is reached
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5));

        messageStream
                .groupBy((k, v) -> v, Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withRetention(Duration.ofSeconds(5))
                )
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count))
                .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
    }
}