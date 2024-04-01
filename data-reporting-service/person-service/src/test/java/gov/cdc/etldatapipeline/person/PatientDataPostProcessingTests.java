package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static gov.cdc.etldatapipeline.person.TestUtils.readFileData;

public class PatientDataPostProcessingTests {
    private static final String FILE_PREFIX = "rawDataFiles/person/";
    @Test
    public void consolidatedPatientTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient p = new Patient();
        p.setNameNested(readFileData(FILE_PREFIX + "PersonName.json"));
        p.setAddressNested(readFileData(FILE_PREFIX +"PersonAddress.json"));
        p.setRaceNested(readFileData(FILE_PREFIX +"PersonRace.json"));
        p.setTelephoneNested(readFileData(FILE_PREFIX +"PersonTelephone.json"));
        p.setEntityDataNested(readFileData(FILE_PREFIX +"PersonEntityData.json"));
        p.setEmailNested(readFileData(FILE_PREFIX +"PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<Object>> pDetailsFn = (pf) -> Arrays.asList(
                pf.getLastNm(),
                pf.getMiddleNm(),
                pf.getFirstNm(),
                pf.getNmSuffix(),
                pf.getStreetAddress1(),
                pf.getStreetAddress2(),
                pf.getCity(),
                pf.getZip(),
                pf.getCountyCode(),
                pf.getCounty(),
                pf.getStateCode(),
                pf.getState(),
                pf.getHomeCountry(),
                pf.getBirthCountry(),
                pf.getPhoneWork(),
                pf.getPhoneExtWork(),
                pf.getPhoneHome(),
                pf.getPhoneExtHome(),
                pf.getPhoneCell(),
                pf.getSsn(),
                pf.getPatientNumber(),
                pf.getPatientNumberAuth(),
                pf.getEmail());

        // Process the respective field json to PatientProvider fields
        PatientReporting pf = p.processPatientReporting();
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
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderNameTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setNameNested(readFileData(FILE_PREFIX +"PersonName.json"));

        // PatientProviderProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix());
        // Process the respective field json to PatientProviderProvider fields
        PatientReporting pf = patient.processPatientReporting();
        // Expected
        List<String> expected = Arrays.asList(
                "Singgh",
                "Js",
                "Suurma",
                "Jr");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderAddressTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient perOp = new Patient();
        perOp.setAddressNested(readFileData(FILE_PREFIX +"PersonAddress.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getStreetAddress1(),
                p.getStreetAddress2(),
                p.getCity(),
                p.getZip(),
                p.getCountyCode(),
                p.getCounty(),
                p.getStateCode(),
                p.getState(),
                p.getHomeCountry(),
                p.getBirthCountry());
        // Process the respective field json to PatientProvider fields
        PatientReporting pf = perOp.processPatientReporting();
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
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderTelephoneTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setTelephoneNested(readFileData(FILE_PREFIX +"PersonTelephone.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneHome(),
                p.getPhoneExtHome(),
                p.getPhoneCell());
        // Process the respective field json to PatientProvider fields
        PatientReporting pf = patient.processPatientReporting();
        // Expected
        List<String> expected = Arrays.asList(
                "2323222422",
                "232",
                "4562323222",
                "211",
                "2823252423");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }


    @Test
    public void PatientProviderEntityDataTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setEntityDataNested(readFileData(FILE_PREFIX +"PersonEntityData.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getSsn(),
                p.getPatientNumber(),
                p.getPatientNumberAuth());

        // Process the respective field json to PatientProvider fields
        PatientReporting pf = patient.processPatientReporting();
        // Expected
        List<String> expected = List.of(
                "313431144414",
                "56743114514",
                "2.16.740.1.113883.3.1147.1.1002");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderEmailTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setEmailNested(readFileData(FILE_PREFIX +"PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmail());

        // Process the respective field json to PatientProvider fields
        PatientReporting pf = patient.processPatientReporting();
        // Expected
        List<String> expected = List.of("someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }
}
