package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.NbsPage;
import gov.cdc.etldatapipeline.changedata.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;



@Service
@Slf4j
public class KafkaStreamsService {
    @Value("#{kafkaConfig.nbsPagesTopicName()}")
    private String nbsPagesTopicName;

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final Serde<NbsPage> NBS_PAGE_SERDE =
            StreamsSerdes.NbsPageSerde();


    private static final String NBS_PAGES_STATE_STORE = "nbs-pages-state-store";

    private final UtilHelper utilHelper = UtilHelper.getInstance();

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        KStream<String, NbsPage> nbsPagesSourceInputKStream =
                streamsBuilder.stream(
                                nbsPagesTopicName,
                                Consumed.with(STRING_SERDE, STRING_SERDE))
                        .map((k, v) -> new KeyValue<>(
                                utilHelper.parseJsonNode(k,
                                        "/payload/nbs_page_uid", String.class),
                                utilHelper.parseJsonNode(v,
                                        "/payload/after", NbsPage.class)));

        // Print nbs_pages stream
        nbsPagesSourceInputKStream.foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        KTable<String, NbsPage> nbsPageTable
                = nbsPagesSourceInputKStream.toTable(
                Materialized.<String, NbsPage, KeyValueStore<Bytes, byte[]>>as(
                                NBS_PAGES_STATE_STORE)
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(NBS_PAGE_SERDE));
        nbsPageTable.toStream().foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        nbsPageTable.toStream().to("nbsPageTransformed",
                Produced.with(STRING_SERDE, NBS_PAGE_SERDE));
    }
}
