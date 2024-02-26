package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.dto.Provider;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import gov.cdc.etldatapipeline.changedata.repository.ProviderRepository;
import gov.cdc.etldatapipeline.changedata.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class ProviderService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final UtilHelper utilHelper = UtilHelper.getInstance();

    private final String personTopicName;
    private final String providerOutputTopicName;
    private final ProviderRepository providerRepository;

    /**
     * @param streamsBuilder
     * From incoming Provider related topic capture the changes
     * Compute the aggregated Provider data
     * Publish the Provider data to aggregated Kafka output topic
     * Steps:
     * 1. Capture the `Provider` topic as Kafka Stream - KStream<String,String>
     * 2. Transform to KStream<String, Person>
     * 3. For each Person call the `sp_provider_event` stored proc &
     *      transform the results to KStream<String,List<Provider>
     * 4. Transform the above result to KStream<Provider.UID,Provider>
     * 5. Write the consolidated `Provider` data to aggregated `Provider` Kafka topic
     */

    public void processProviderData(StreamsBuilder streamsBuilder) {
        KStream<String, Person> personKStream
                = streamsBuilder.stream(personTopicName, Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> new KeyValue<>(
                        k,
                        utilHelper.deserializePayload(v, "/payload/after", Person.class)))
                // KStream<String, Person>
                .peek((key, value) -> log.info("Calling the Provider Repository for " + value.getPersonUid()));

        KStream<String, Provider> consolidatedProviderStream = personKStream
                .mapValues(v -> providerRepository.computeAllProviders(v.getPersonUid()))
                //KStream<String, List<Provider>>
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(p.getProviderUid(), p))
                        .collect(Collectors.toSet()))
                // KStream<String, Provider>
                .peek((key, value) -> log.info("Provider Info : {}", value.toString()));

        consolidatedProviderStream.to(providerOutputTopicName,
                Produced.with(Serdes.String(), StreamsSerdes.ProviderSerde()));
    }
}

/**
 * NBS_ODSE.dbo.PERSON, NBS_ODSE.dbo.PERSON_NAME,
 * ENTITY_LOCATOR_PARTICIPATION, NBS_ODSE.dbo.POSTAL_LOCATOR, NBS_ODSE.dbo
 * .TELE_LOCATOR
 * 1. TMP_S_PROVIDER_INIT - Get all persons - NBS_ODSE.dbo.PERSON, who are
 * type provider that got changed since last run
 * 2. TMP_PROVIDER_UID_COLL - Collect all the Person_UID from
 * TMP_S_PROVIDER_INIT (Step 1)
 * 3. TMP_S_INITPROVIDER - Provider + AuthUser - Join TMP_S_PROVIDER_INIT and
 * AuthUser - Get Provider Added by/Changed by
 * 4. TMP_S_PROVIDER_NAME - Extract Provider Names by joining
 * TMP_PROVIDER_UID_COLL + NBS_ODSE.dbo.PERSON_NAME
 * 5. TMP_S_PROVIDER_WITH_NM - Join TMP_S_PROVIDER_INIT + TMP_S_PROVIDER_NAME
 * 6. TMP_S_PROVIDER_POSTAL_LOCATOR - Get provider address
 * 7. TMP_S_PROVIDER_TELE_LOCATOR_OFFICE - Get provider office telephone contact
 * 8. TMP_S_PROVIDER_TELE_LOCATOR_CELL -  Provider Cell telephone contact
 * 9. TMP_S_PROVIDER_LOCATOR - Join TMP_S_PROVIDER_TELE_LOCATOR_OFFICE,
 * TMP_S_PROVIDER_TELE_LOCATOR_CELL, TMP_S_PROVIDER_POSTAL_LOCATOR
 * 10. TMP_S_PROVIDER_QEC_ENTITY_ID - Get provider of entity type `QEC`
 * 11. TMP_PRN_ENTITY_ID - Get provider of entity type `PRN`
 * 12. TMP_S_PROVIDER_FINAL - Join TMP_S_PROVIDER_WITH_NM (step 5),
 * TMP_S_PROVIDER_LOCATOR (step 9), TMP_S_PROVIDER_QEC_ENTITY_ID (step 10),
 * TMP_PRN_ENTITY_ID (step 11)
 * 13. S_PROVIDER - Copy TMP_S_PROVIDER_FINAL to S_PROVIDER
 * ---------
 * 14. TMP_L_PROVIDER_N
 * 15. TMP_L_PROVIDER_E
 * ---
 * 16. TMP_D_PROVIDER_N- Join S_PROVIDER, TMP_L_PROVIDER_N
 * 17. TMP_D_PROVIDER_E - Join S_PROVIDER, TMP_D_PROVIDER_E
 * 18. Update D_PROVIDER - Update from TMP_D_PROVIDER_E
 * 19. Insert into D_PROVIDER - from TMP_D_PROVIDER_N
 ***/
