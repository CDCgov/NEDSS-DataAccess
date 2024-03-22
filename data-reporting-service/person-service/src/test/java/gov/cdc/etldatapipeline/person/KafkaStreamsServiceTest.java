package gov.cdc.etldatapipeline.person;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.model.dto.DataEnvelope;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientFull;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientKey;
import gov.cdc.etldatapipeline.person.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.person.model.dto.provider.Provider;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderFull;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderKey;
import gov.cdc.etldatapipeline.person.repository.PatientRepository;
import gov.cdc.etldatapipeline.person.repository.ProviderRepository;
import gov.cdc.etldatapipeline.person.service.KafkaStreamsService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
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

import java.io.File;
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
    private final String patientTopic = "PatientTopic";
    private final String providerTopic = "ProviderTopic";
    private final String defaultTopic = "DefaultTopic";


    @Test
    public void processPatientData() throws IOException {
        // Generate a Debezium Person Patient Change Data
        File file = new File("src/test/resources/rawDataFiles/PersonPatientChangeData.json");
        String personPatientOdseData = FileUtils.readFileToString(file,
                Charset.defaultCharset());

        Patient patient = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patient));
        //Build expected unflattened Provider
        PatientFull expectedPf = new PatientFull().constructPersonFull(patient);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);
        //Construct expected Patient Key
        PatientKey expectedPatientKey = new PatientKey(expectedPf.getPersonUid());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(personTopic, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(patientTopic, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", personPatientOdseData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Patient Payload
            TypeReference<DataEnvelope<PatientFull>> patientFullDataEnv = new TypeReference<>() {};

            DataEnvelope<PatientFull> actualValue
                    = new ObjectMapper().readValue(actualData.get(0).value, patientFullDataEnv);
            Assertions.assertEquals(expectedPf, actualValue.getPayload());
            Assertions.assertEquals(JsonSchemaUtils.getSchema(expectedPf).toJsonNode(), actualValue.getSchema());

            //Validate the Patient Key
            TypeReference<DataEnvelope<PatientKey>> patientKeyDataEnv = new TypeReference<>() {};
            DataEnvelope<PatientKey> actualKey
                    = new ObjectMapper().readValue(actualData.get(0).key, patientKeyDataEnv);
            Assertions.assertEquals(expectedPatientKey, actualKey.getPayload());
            Assertions.assertEquals(JsonSchemaUtils.getSchema(expectedPatientKey).toJsonNode(), actualKey.getSchema());

        }
    }

    @Test
    public void processProviderData() throws IOException {
        // Generate a Debezium Person Patient Change Data
        File file = new File("src/test/resources/rawDataFiles/PersonProviderChangeData.json");
        String personPatientOdseData = FileUtils.readFileToString(file,
                Charset.defaultCharset());

        Provider constructedProvider = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProvider));

        //Build expected unflattened Provider
        ProviderFull expectedPf = new ProviderFull().constructProviderFull(constructedProvider);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);
        //Construct expected Provider Key
        ProviderKey expectedProviderKey = new ProviderKey(expectedPf.getPersonUid());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsService ks = getKafkaStreamService();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(personTopic, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(providerTopic, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", personPatientOdseData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Provider Payload
            TypeReference<DataEnvelope<ProviderFull>> providerFullDataEnv = new TypeReference<>() {};
            DataEnvelope<ProviderFull> actualValue
                    = new ObjectMapper().readValue(actualData.get(0).value, providerFullDataEnv);
            Assertions.assertEquals(expectedPf, actualValue.getPayload());
            Assertions.assertEquals(JsonSchemaUtils.getSchema(expectedPf).toJsonNode(), actualValue.getSchema());

            //Validate the Provider Key
            TypeReference<DataEnvelope<ProviderKey>> providerKeyDataEnv = new TypeReference<>() {};
            DataEnvelope<ProviderKey> actualKey
                    = new ObjectMapper().readValue(actualData.get(0).key, providerKeyDataEnv);
            Assertions.assertEquals(expectedProviderKey, actualKey.getPayload());
            Assertions.assertEquals(JsonSchemaUtils.getSchema(expectedProviderKey).toJsonNode(), actualKey.getSchema());

        }
    }

    private KafkaStreamsService getKafkaStreamService() {
        KafkaStreamsService ks = new KafkaStreamsService(patientRepository, providerRepository);
        ks.setPersonTopicName(personTopic);
        ks.setPatientOutputTopicName(patientTopic);
        ks.setProviderOutputTopicName(providerTopic);
        ks.setDefaultDataTopicName(defaultTopic);
        return ks;
    }

    private Patient constructPatient() {
        Patient p = new Patient();
        p.setPersonUid(10000001L);
        p.setName(readFileData("PersonName.json"));
        p.setAddress(readFileData("PersonAddress.json"));
        p.setRace(readFileData("PersonRace.json"));
        p.setTelephone(readFileData("PersonTelephone.json"));
        p.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        p.setChgAuthNested(readFileData("PersonChgAuthUser.json"));
        p.setEntityData(readFileData("PersonEntityData.json"));
        p.setEmailNested(readFileData("PersonEmail.json"));
        return p;
    }

    private Provider constructProvider() {
        Provider p = new Provider();
        p.setPersonUid(10000001L);
        p.setName(readFileData("PersonName.json"));
        p.setAddress(readFileData("PersonAddress.json"));
        p.setTelephone(readFileData("PersonTelephone.json"));
        p.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        p.setChgAuthNested(readFileData("PersonChgAuthUser.json"));
        p.setEntityData(readFileData("PersonEntityData.json"));
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


        AddAuthUser addAuthUser = new AddAuthUser();
        addAuthUser.setAddUserId(10000000L);
        addAuthUser.updatePerson(patProv);


        ChgAuthUser chgAuthUser = new ChgAuthUser();
        chgAuthUser.setLastChgUserId(470200741L);
        chgAuthUser.updatePerson(patProv);


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
