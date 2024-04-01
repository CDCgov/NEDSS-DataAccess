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
    private final String providerElasticTopic = "ProviderElasticTopic";
    private final String providerReportingTopic = "ProviderReportingTopic";
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void processPatientReportingData() {
        Patient patient = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patient));
        //Build expected unflattened Provider
        PatientReporting expectedPf = new PatientReporting().constructObject(patient);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonPatientChangeData.json"),
                personTopic,
                patientReportingTopic,
                "rawDataFiles/patient/PatientReporting.json",
                "rawDataFiles/patient/PatientKey.json");
    }

    @Test
    public void processPatientElasticSearchData() {
        Patient patient = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patient));
        //Build expected unflattened Provider
        PatientReporting expectedPf = new PatientReporting().constructObject(patient);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);

        // Validate Patient ElasticSearch Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonPatientChangeData.json"),
                personTopic,
                patientElasticTopic,
                "rawDataFiles/patient/PatientElastic.json",
                "rawDataFiles/patient/PatientKey.json");
    }

    @Test
    public void processProviderReportingData() {
        Provider constructedProvider = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProvider));

        //Build expected unflattened Provider
        ProviderReporting expectedPf = new ProviderReporting().constructObject(constructedProvider);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonProviderChangeData.json"),
                personTopic,
                providerReportingTopic,
                "rawDataFiles/provider/ProviderReporting.json",
                "rawDataFiles/provider/ProviderKey.json");
    }

    @Test
    public void processProviderElasticSearchData() {
        Provider constructedProvider = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProvider));

        //Build expected unflattened Provider
        ProviderReporting expectedPf = new ProviderReporting().constructObject(constructedProvider);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonProviderChangeData.json"),
                personTopic,
                providerElasticTopic,
                "rawDataFiles/provider/ProviderElasticSearch.json",
                "rawDataFiles/provider/ProviderKey.json");
    }

    /**
     * Create a mock Kafka cluster and do stream processing of the Patient/Provider data
     *
     * @param incomingChangeData    Debezium Change Data
     * @param inputTopicName        Input Topic to monitor
     * @param outputTopicName       Output Topic to produce the transformed data
     * @param expectedValueFilePath Expected transformed Json Value Data in the DataEnvelope format
     * @param expectedKeyFilePath   Expected transformed Json Key Data in the DataEnvelope format
     */
    private void validateDataTransformation(
            String incomingChangeData,
            String inputTopicName,
            String outputTopicName,
            String expectedValueFilePath,
            String expectedKeyFilePath) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(outputTopicName, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", incomingChangeData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Provider Payload
            TypeReference<DataEnvelope> dataEnvelopeTypeReference = new TypeReference<>() {
            };

            DataEnvelope<DataEnvelope> actualValue
                    = objectMapper.readValue(actualData.get(0).value, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource(expectedValueFilePath).getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualValue);

            //Validate the Patient Key
            DataEnvelope<DataEnvelope> actualKey
                    = objectMapper.readValue(actualData.get(0).key, dataEnvelopeTypeReference);
            //Construct expected Patient Key
            Assertions.assertEquals(
                    objectMapper.readValue(FileUtils.readFileToString(
                                    new ClassPathResource(expectedKeyFilePath).getFile(),
                                    Charset.defaultCharset()),
                            dataEnvelopeTypeReference),
                    actualKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private KafkaStreamsService getKafkaStreamService() {
        KafkaStreamsService ks = new KafkaStreamsService(patientRepository, providerRepository);
        ks.setPersonTopicName(personTopic);
        ks.setPatientElasticSearchTopicName(patientElasticTopic);
        ks.setPatientReportingOutputTopic(patientReportingTopic);
        ks.setProviderReportingOutputTopic(providerReportingTopic);
        ks.setProviderElasticSearchOutputTopic(providerElasticTopic);
        return ks;
    }

    private Patient constructPatient() {
        Patient p = new Patient();
        p.setPatientUid(10000001L);
        String filePathPrefix = "rawDataFiles/person/";
        p.setNameNested(readFileData(filePathPrefix + "PersonName.json"));
        p.setAddressNested(readFileData(filePathPrefix + "PersonAddress.json"));
        p.setRaceNested(readFileData(filePathPrefix + "PersonRace.json"));
        p.setTelephoneNested(readFileData(filePathPrefix + "PersonTelephone.json"));
        p.setEntityDataNested(readFileData(filePathPrefix + "PersonEntityData.json"));
        p.setEmailNested(readFileData(filePathPrefix + "PersonEmail.json"));
        return p;
    }

    private Provider constructProvider() {
        Provider p = new Provider();
        p.setPersonUid(10000001L);
        String filePathPrefix = "rawDataFiles/person/";
        p.setNameNested(readFileData(filePathPrefix + "PersonName.json"));
        p.setAddressNested(readFileData(filePathPrefix + "PersonAddress.json"));
        p.setTelephoneNested(readFileData(filePathPrefix + "PersonTelephone.json"));
        p.setEntityDataNested(readFileData(filePathPrefix + "PersonEntityData.json"));
        p.setEmailNested(readFileData(filePathPrefix + "PersonEmail.json"));
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
