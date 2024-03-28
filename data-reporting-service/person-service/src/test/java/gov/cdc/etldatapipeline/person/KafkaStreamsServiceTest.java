package gov.cdc.etldatapipeline.person;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.model.dto.DataProps.DataEnvelope;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import gov.cdc.etldatapipeline.person.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.person.model.dto.provider.Provider;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderReporting;
import gov.cdc.etldatapipeline.person.repository.PatientRepository;
import gov.cdc.etldatapipeline.person.repository.ProviderRepository;
import gov.cdc.etldatapipeline.person.service.KafkaStreamsService;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import static gov.cdc.etldatapipeline.person.TestUtils.readFileData;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
public class KafkaStreamsServiceTest {

    @Mock
    PatientRepository patientRepository;

    @Mock
    ProviderRepository providerRepository;

    private final String personTopic = "PersonTopic";
    private final String patientElasticTopic = "PatientElasticTopic";
    private final String patientReportingTopic = "PatientReportingTopic";
    private final String providerTopic = "ProviderTopic";
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void processPatientData() throws IOException {
        // Generate a Debezium Person Patient Change Data
        String personPatientOdseData = FileUtils.readFileToString(
                new ClassPathResource("rawDataFiles/PersonPatientChangeData.json").getFile(),
                Charset.defaultCharset());

        Patient patient = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patient));
        //Build expected unflattened Provider
        PatientReporting expectedPf = new PatientReporting().constructPatientReporting(patient);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(personTopic, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> patientReportingOutputTopic = topologyTestDriver
                    .createOutputTopic(patientReportingTopic, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", personPatientOdseData);
            List<KeyValue<String, String>> actualData = patientReportingOutputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Patient Payload
            TypeReference<DataEnvelope> dataEnvelopeTypeReference = new TypeReference<>() {
            };

            DataEnvelope<PatientReporting> actualValue
                    = objectMapper.readValue(actualData.get(0).value, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource("rawDataFiles/PatientReporting.json").getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualValue);

            //Validate the Patient Key
            DataEnvelope<DataEnvelope> actualKey
                    = objectMapper.readValue(actualData.get(0).key, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource("rawDataFiles/PatientKey.json").getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualKey);
        }
    }

    @Test
    public void processProviderData() throws IOException {
        // Generate a Debezium Person Patient Change Data
        String personProviderOdseData = FileUtils.readFileToString(
                new ClassPathResource("rawDataFiles/PersonProviderChangeData.json").getFile(),
                Charset.defaultCharset());

        Provider constructedProvider = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProvider));

        //Build expected unflattened Provider
        ProviderReporting expectedPf = new ProviderReporting().constructProviderFull(constructedProvider);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(personTopic, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(providerTopic, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", personProviderOdseData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Provider Payload
            TypeReference<DataEnvelope> dataEnvelopeTypeReference = new TypeReference<>() {
            };

            DataEnvelope<DataEnvelope> actualValue
                    = objectMapper.readValue(actualData.get(0).value, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource("rawDataFiles/ProviderReporting.json").getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualValue);

            //Validate the Patient Key
            DataEnvelope<DataEnvelope> actualKey
                    = objectMapper.readValue(actualData.get(0).key, dataEnvelopeTypeReference);
            //Construct expected Patient Key
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource("rawDataFiles/ProviderKey.json").getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualKey);
        }
    }


    private KafkaStreamsService getKafkaStreamService() {
        KafkaStreamsService ks = new KafkaStreamsService(patientRepository, providerRepository);
        ks.setPersonTopicName(personTopic);
        ks.setPatientElasticSearchTopicName(patientElasticTopic);
        ks.setPatientReportingOutputTopic(patientReportingTopic);
        ks.setProviderReportingOutputTopic(providerTopic);
        return ks;
    }

    private Patient constructPatient() {
        Patient p = new Patient();
        p.setPatientUid(10000001L);
        p.setNameNested(readFileData("PersonName.json"));
        p.setAddressNested(readFileData("PersonAddress.json"));
        p.setRaceNested(readFileData("PersonRace.json"));
        p.setTelephoneNested(readFileData("PersonTelephone.json"));
        p.setEntityDataNested(readFileData("PersonEntityData.json"));
        p.setEmailNested(readFileData("PersonEmail.json"));
        return p;
    }

    private Provider constructProvider() {
        Provider p = new Provider();
        p.setPersonUid(10000001L);
        p.setNameNested(readFileData("PersonName.json"));
        p.setAddressNested(readFileData("PersonAddress.json"));
        p.setTelephoneNested(readFileData("PersonTelephone.json"));
        p.setEntityDataNested(readFileData("PersonEntityData.json"));
        p.setEmailNested(readFileData("PersonEmail.json"));
        return p;
    }

    private <T extends PersonExtendedProps> void constructPatPrvFull(T patProv) {
        Name name = new Name();
        name.setLastNm("Singgh");
        name.setMiddleNm("Js");
        name.setFirstNm("Suurma");
        name.setNmSuffix("Jr");
        name.updatePerson(patProv);

        Address address = new Address();
        address.setStreetAddr1("123 Main St.");
        address.setStreetAddr2("");
        address.setCity("Atlanta");
        address.setZip("30025");
        address.setCntyCd("13135");
        address.setCounty("Gwinnett County");
        address.setState("13");
        address.setStateDesc("Georgia");
        address.setCntryCd("840");
        address.setHomeCountry("United States");
        address.setBirthCountry("Canada");
        address.updatePerson(patProv);

        Phone workPhone = new Phone();
        workPhone.setTelephoneNbr("2323222422");
        workPhone.setExtensionTxt("232");
        workPhone.setUseCd("WP");
        workPhone.updatePerson(patProv);

        Phone homePhone = new Phone();
        homePhone.setTelephoneNbr("4562323222");
        homePhone.setExtensionTxt("211");
        homePhone.setUseCd("H");
        homePhone.updatePerson(patProv);


        Phone cellPhone = new Phone();
        cellPhone.setTelephoneNbr("2823252423");
        cellPhone.setUseCd("CP");
        cellPhone.updatePerson(patProv);


        Race race = new Race();
        race.setRaceCd("2028-9");
        race.setRaceCategoryCd("2028-9");
        race.setRaceDescTxt("Amer Indian");
        race.updatePerson(patProv);


        EntityData ssa = new EntityData();
        ssa.setRootExtensionTxt("313431144414");
        ssa.setAssigningAuthorityCd("SSA");
        ssa.updatePerson(patProv);


        EntityData pn = new EntityData();
        pn.setTypeCd("PN");
        pn.setRootExtensionTxt("56743114514");
        pn.setAssigningAuthorityCd("2.16.740.1.113883.3.1147.1.1002");
        pn.updatePerson(patProv);


        EntityData qec = new EntityData();
        qec.setTypeCd("QEC");
        qec.setRootExtensionTxt("12314286");
        qec.updatePerson(patProv);

        EntityData regNum = new EntityData();
        regNum.setTypeCd("PRN");
        regNum.setRootExtensionTxt("86741517517");
        regNum.setAssigningAuthorityCd("3.16.740.1.113883.3.1147.1.1002");
        regNum.updatePerson(patProv);


        Email email = new Email();
        email.setEmailAddress("someone2@email.com");
        email.updatePerson(patProv);
    }

}
