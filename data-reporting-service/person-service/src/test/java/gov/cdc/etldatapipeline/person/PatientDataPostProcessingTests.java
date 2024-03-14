package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.dto.patient.PatientFull;
import gov.cdc.etldatapipeline.person.model.dto.patient.Patient;
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
        p.setName(readFileData("PersonName.json"));
        p.setAddress(readFileData("PersonAddress.json"));
        p.setRace(readFileData("PersonRace.json"));
        p.setTelephone(readFileData("PersonTelephone.json"));
        p.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        p.setChgAuthNested(readFileData("PersonChgAuthUser.json"));
        p.setEntityData(readFileData("PersonEntityData.json"));
        p.setEmail(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<Object>> pDetailsFn = (pf) -> Arrays.asList(
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
                pf.getCountryCode(),
                pf.getCountry(),
                pf.getBirthCountry(),
                pf.getPhoneWork(),
                pf.getPhoneExtWork(),
                pf.getPhoneHome(),
                pf.getPhoneExtHome(),
                pf.getPhoneCell(),
                pf.getRaceCd(),
                pf.getRaceCategory(),
                pf.getRaceDesc(),
                pf.getAddedBy(),
                pf.getLastChangedBy(),
                pf.getSsn(),
                pf.getPatientNumber(),
                pf.getPatientNumberAuth(),
                pf.getProviderQuickCode(),
                pf.getProviderRegistrationNum(),
                pf.getProviderRegistrationNumAuth(),
                pf.getEmail());

        // Process the respective field json to PatientProvider fields
        PatientFull pf = p.processPatient();
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
                "840",
                "United States",
                "Canada",
                "2323222422",
                "232",
                "4562323222",
                "211",
                "2823252423",
                "2028-9",
                "2028-9",
                "Amer Indian",
                10000000L,
                470200741L,
                "313431144414",
                "56743114514",
                "2.16.740.1.113883.3.1147.1.1002",
                "12314286",
                "86741517517",
                "3.16.740.1.113883.3.1147.1.1002",
                "someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderNameTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setName(readFileData("PersonName.json"));

        // PatientProviderProvider Fields to be processed
        Function<Patient, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix());
        // Process the respective field json to PatientProviderProvider fields
        PatientFull pf = patient.processPatient();
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
        perOp.setAddress(readFileData("PersonAddress.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getStreetAddress1(),
                p.getStreetAddress2(),
                p.getCity(),
                p.getZip(),
                p.getCountyCode(),
                p.getCounty(),
                p.getStateCode(),
                p.getState(),
                p.getCountryCode(),
                p.getCountry(),
                p.getBirthCountry());
        // Process the respective field json to PatientProvider fields
        PatientFull pf = perOp.processPatient();
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
                "840",
                "United States",
                "Canada");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderRaceTransformationTest() {
        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setRace(readFileData("PersonRace.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getRaceCd(),
                p.getRaceCategory(),
                p.getRaceDesc());
        // Process the respective field json to PatientProvider fields
        PatientFull pf = patient.processPatient();
        // Expected
        List<String> expected = Arrays.asList(
                "2028-9",
                "2028-9",
                "Amer Indian");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderTelephoneTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setTelephone(readFileData("PersonTelephone.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneHome(),
                p.getPhoneExtHome(),
                p.getPhoneCell());
        // Process the respective field json to PatientProvider fields
        PatientFull pf = patient.processPatient();
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
    public void PatientProviderAddChangeAuthUserTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        patient.setChgAuthNested(readFileData("PersonChgAuthUser.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<Long>> pDetailsFn = (p) -> Arrays.asList(
                p.getAddedBy(),
                p.getLastChangedBy());
        // Process the respective field json to PatientProvider fields
        PatientFull pf = patient.processPatient();
        // Expected
        List<Long> expected = Arrays.asList(
                10000000L,
                470200741L);
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderEntityDataTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setEntityData(readFileData("PersonEntityData.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getSsn(),
                p.getPatientNumber(),
                p.getPatientNumberAuth(),
                p.getProviderQuickCode(),
                p.getProviderRegistrationNum(),
                p.getProviderRegistrationNumAuth());

        // Process the respective field json to PatientProvider fields
        PatientFull pf = patient.processPatient();
        // Expected
        List<String> expected = List.of(
                "313431144414",
                "56743114514",
                "2.16.740.1.113883.3.1147.1.1002",
                "12314286",
                "86741517517",
                "3.16.740.1.113883.3.1147.1.1002");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderEmailTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Patient patient = new Patient();
        patient.setEmail(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<PatientFull, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmail());

        // Process the respective field json to PatientProvider fields
        PatientFull pf = patient.processPatient();
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
