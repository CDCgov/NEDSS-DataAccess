package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.odse.NbsPage;
import gov.cdc.etldatapipeline.changedata.repository.PageRepository;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

@RequiredArgsConstructor
@Slf4j
public class NbsPageService {
    private final String nbsPagesTopicName;
    private final PageRepository pageRepository;

    public void processNbsPage(StreamsBuilder streamsBuilder) {
        UtilHelper utilHelper = UtilHelper.getInstance();
        KStream<String, NbsPage> nbsPagesSourceInputKStream =
                streamsBuilder.stream(
                                nbsPagesTopicName,
                                Consumed.with(Serdes.String(), Serdes.String()))
                        .map((k, v) -> new KeyValue<>(
                                utilHelper.parseJsonNode(k,
                                        "/payload/nbs_page_uid",
                                        String.class),
                                utilHelper.deserializePayload(v,
                                "/payload/after",
                                NbsPage.class)));
        // Print nbs_pages stream
        nbsPagesSourceInputKStream.foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        // Print nbs_pages stream
        nbsPagesSourceInputKStream.foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        /*KTable<String, NbsPage> nbsPageTable
                = nbsPagesSourceInputKStream.toTable(
                Materialized.<String, NbsPage, KeyValueStore<Bytes, byte[]>>as(
                                NBS_PAGES_STATE_STORE)
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(NBS_PAGE_SERDE));
        nbsPageTable.toStream().foreach((k, v) -> log.info(
                "nbsPagesSourceInputKStream :: Key :: {}, Value :: {}", k, v));

        nbsPageTable.toStream().to("nbsPageTransformed",
                Produced.with(STRING_SERDE, NBS_PAGE_SERDE));*/

        nbsPagesSourceInputKStream.foreach((k, v) -> pageRepository.save(v));
    }
}
