package gov.cdc.etldatapipeline.person;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientSp;
import gov.cdc.etldatapipeline.person.transformer.PersonTransformers;
import gov.cdc.etldatapipeline.person.transformer.PersonType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static gov.cdc.etldatapipeline.commonutil.TestUtils.readFileData;

public class PatientDataPostProcessingTests {
    private static final String FILE_PREFIX = "rawDataFiles/person/";
    PersonTransformers tx = new PersonTransformers();

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void consolidatedPatientTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp pat = PatientSp.builder()
                .personUid(10000001L)
                .nameNested(readFileData(FILE_PREFIX + "PersonName.json"))
                .addressNested(readFileData(FILE_PREFIX + "PersonAddress.json"))
                .raceNested(readFileData(FILE_PREFIX + "PersonRace.json"))
                .telephoneNested(readFileData(FILE_PREFIX + "PersonTelephone.json"))
                .entityDataNested(readFileData(FILE_PREFIX + "PersonEntityData.json"))
                .emailNested(readFileData(FILE_PREFIX + "PersonEmail.json"))
                .build();

        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(pat, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("last_nm").asText(),
                payloadNode.get("middle_nm").asText(),
                payloadNode.get("first_nm").asText(),
                payloadNode.get("nm_suffix").asText(),
                payloadNode.get("street_address1").asText(),
                payloadNode.get("street_address2").asText(),
                payloadNode.get("city").asText(),
                payloadNode.get("zip").asText(),
                payloadNode.get("county_code").asText(),
                payloadNode.get("county").asText(),
                payloadNode.get("state_code").asText(),
                payloadNode.get("state").asText(),
                payloadNode.get("home_country").asText(),
                payloadNode.get("birth_country").asText(),
                payloadNode.get("phone_work").asText(),
                payloadNode.get("phone_ext_work").asText(),
                payloadNode.get("phone_home").asText(),
                payloadNode.get("phone_ext_home").asText(),
                payloadNode.get("phone_cell").asText(),
                payloadNode.get("ssn").asText(),
                payloadNode.get("patient_number").asText(),
                payloadNode.get("patient_number_auth").asText(),
                payloadNode.get("email").asText());

        // Expected
        List<Object> expected = Arrays.asList(
                "Singgh",
                "Js",
                "Suurma",
                "Jr",
                "123 Main St.",
                "",
                "Atlanta",
                "30025",
                "13135",
                "Gwinnett County",
                "13",
                "Georgia",
                "United States",
                "Canada",
                "2323222422",
                "232",
                "4562323222",
                "211",
                "2823252423",
                "313431144414",
                "56743114514",
                "2.16.740.1.113883.3.1147.1.1002",
                "someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientNameTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .nameNested(readFileData(FILE_PREFIX + "PersonName.json"))
                .build();

        // Process the respective field json to PatientProviderProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("last_nm").asText(),
                payloadNode.get("middle_nm").asText(),
                payloadNode.get("first_nm").asText(),
                payloadNode.get("nm_suffix").asText(),
                payloadNode.get("alias_nickname").asText());

        // Expected
        List<String> expected = Arrays.asList(
                "Singgh",
                "Js",
                "Suurma",
                "Jr",
                "null");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientNameTransformationSet2Test() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .nameNested(readFileData(FILE_PREFIX + "PersonName1.json"))
                .build();

        // Patient Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix(),
                p.getAliasNickname());

        // Process the respective field json to PatientProviderProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("last_nm").asText(),
                payloadNode.get("middle_nm").asText(),
                payloadNode.get("first_nm").asText(),
                payloadNode.get("nm_suffix").asText(),
                payloadNode.get("alias_nickname").asText());

        List<String> expected = Arrays.asList(
                "jack",
                "amy",
                "beans",
                "Sr",
                "XEZD6SLFPRUJQGA52");
        // Validate the Patient field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientProviderAddressTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .addressNested(readFileData(FILE_PREFIX + "PersonAddress.json"))
                .build();

        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("street_address1").asText(),
                payloadNode.get("street_address2").asText(),
                payloadNode.get("city").asText(),
                payloadNode.get("zip").asText(),
                payloadNode.get("county_code").asText(),
                payloadNode.get("county").asText(),
                payloadNode.get("state_code").asText(),
                payloadNode.get("state").asText(),
                payloadNode.get("home_country").asText(),
                payloadNode.get("birth_country").asText());

        // Expected
        List<String> expected = Arrays.asList(
                "123 Main St.",
                "",
                "Atlanta",
                "30025",
                "13135",
                "Gwinnett County",
                "13",
                "Georgia",
                "United States",
                "Canada");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientProviderTelephoneTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .telephoneNested(readFileData(FILE_PREFIX + "PersonTelephone.json"))
                .build();

        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("phone_work").asText(),
                payloadNode.get("phone_ext_work").asText(),
                payloadNode.get("phone_home").asText(),
                payloadNode.get("phone_ext_home").asText(),
                payloadNode.get("phone_cell").asText());

        // Expected
        List<String> expected = Arrays.asList(
                "2323222422",
                "232",
                "4562323222",
                "211",
                "2823252423");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }


    @Test
    public void PatientProviderEntityDataTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .entityDataNested(readFileData(FILE_PREFIX + "PersonEntityData.json"))
                .build();

        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("ssn").asText(),
                payloadNode.get("patient_number").asText(),
                payloadNode.get("patient_number_auth").asText());

        List<String> expected = List.of(
                "313431144414",
                "56743114514",
                "2.16.740.1.113883.3.1147.1.1002");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientProviderEmailTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .emailNested(readFileData(FILE_PREFIX + "PersonEmail.json"))
                .build();

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmail());

        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");

        List<String> actual = Arrays.asList(payloadNode.get("email").asText());

        // Expected
        List<String> expected = List.of("someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void PatientRaceBreakdownTransformationTest() throws JsonProcessingException {

        // Build the PatientProvider object with the json serialized data
        PatientSp perOp = PatientSp.builder()
                .personUid(10000001L)
                .raceNested(readFileData(FILE_PREFIX + "PersonRace.json"))
                .build();

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getRaceCalculated(),
                p.getRaceCalcDetails(),
                p.getRaceAll(),
                p.getRaceAmerInd1(),
                p.getRaceAmerInd2(),
                p.getRaceAmerInd3(),
                p.getRaceAmerIndGt3Ind(),
                p.getRaceAmerIndAll(),
                p.getRaceAsian1(),
                p.getRaceAsian2(),
                p.getRaceAsian3(),
                p.getRaceAsianGt3Ind(),
                p.getRaceAsianAll(),
                p.getRaceBlack1(),
                p.getRaceBlack2(),
                p.getRaceBlack3(),
                p.getRaceBlackGt3Ind(),
                p.getRaceBlackAll(),
                p.getRaceNatHi1(),
                p.getRaceNatHi2(),
                p.getRaceNatHi3(),
                p.getRaceNatHiGt3Ind(),
                p.getRaceNatHiAll(),
                p.getRaceWhite1(),
                p.getRaceWhite2(),
                p.getRaceWhite3(),
                p.getRaceWhiteGt3Ind(),
                p.getRaceWhiteAll());
        // Process the respective field json to PatientProvider fields
        String processedData = tx.processData(perOp, PersonType.PATIENT_REPORTING);
        JsonNode payloadNode = objectMapper.readTree(processedData).get("payload");
        PatientReporting pf = objectMapper.treeToValue(payloadNode, PatientReporting.class);

        // Expected
        List<String> expected = Arrays.asList(
                "Asian",
                "Asian",
                "Asian",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }
}
