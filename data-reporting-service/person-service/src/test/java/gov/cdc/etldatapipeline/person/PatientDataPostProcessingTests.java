package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class PatientDataPostProcessingTests {
    @Test
    public void consolidatedPatientTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient p = new Patient();
        p.setNameNested(readFileData("PersonName.json"));
        p.setAddressNested(readFileData("PersonAddress.json"));
        p.setRaceNested(readFileData("PersonRace.json"));
        p.setTelephoneNested(readFileData("PersonTelephone.json"));
        p.setEntityDataNested(readFileData("PersonEntityData.json"));
        p.setEmailNested(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<Object>> pDetailsFn = (pf) -> Arrays.asList(
                pf.getLastName(),
                pf.getMiddleName(),
                pf.getFirstName(),
                pf.getNameSuffix(),
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
        patient.setNameNested(readFileData("PersonName.json"));

        // PatientProviderProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastName(),
                p.getMiddleName(),
                p.getFirstName(),
                p.getNameSuffix());
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
        perOp.setAddressNested(readFileData("PersonAddress.json"));

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
        patient.setTelephoneNested(readFileData("PersonTelephone.json"));

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
        patient.setEntityDataNested(readFileData("PersonEntityData.json"));

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
        patient.setEmailNested(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientReporting, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmail());

        // Process the respective field json to PatientProvider fields
        PatientReporting pf = patient.processPatientReporting();
        // Expected
        List<String> expected = List.of("someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    private String readFileData(String fileName) {
        String filePathPrefix = "src/test/resources/rawDataFiles/";
        try {
            return FileUtils.readFileToString(
                    new File(filePathPrefix + fileName),
                    Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("File Read failed : " + fileName);
        }
    }
}
