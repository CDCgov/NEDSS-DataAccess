package gov.cdc.etldatapipeline.changedata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaStreamsService {
    @Value("#{kafkaConfig.nbsPagesTopicName()}")
    private String nbsPagesTopicName;

    @Value("#{kafkaConfig.personTopicName()}")
    private String personTopicName;

    @Value("#{kafkaConfig.participationTopicName()}")
    private String participationTopicName;

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final String NBS_PAGES_STATE_STORE = "nbs-pages-state-store";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        KStream<String, String> nbsPagesSourceInputKStream =
                streamsBuilder.stream(
                        nbsPagesTopicName,
                        Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> personSourceInputKStream =
                streamsBuilder.stream(
                        personTopicName,
                        Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> participationSourceInputKStream =
                streamsBuilder.stream(
                        participationTopicName,
                        Consumed.with(STRING_SERDE, STRING_SERDE));

        // Print nbs_pages stream
        nbsPagesSourceInputKStream.foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        personSourceInputKStream.foreach((k, v) -> log.info(
                "personSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        participationSourceInputKStream.foreach((k, v) -> log.info(
                "participationSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        /*ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> {
            return leftValue + rightValue;
        };
        KStream<String, String> joined = personSourceInputKStream.join(participationSourceInputKStream,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30))
        );

        //joined.to("my-kafka-stream-stream-inner-join-out");
        joined.foreach((k, v) -> {
            System.out.println("JOIN STREAM: KEY="+k+ ", VALUE=" + v);
        }); */


    }
}
