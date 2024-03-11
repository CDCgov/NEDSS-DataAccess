package gov.cdc.etldatapipeline;

import gov.cdc.etldatapipeline.changedata.model.dto.PersonOp;
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

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setName(readFileData("PatientName.json"));
        p.setAddress(readFileData("PatientAddress.json"));
        p.setRace(readFileData("PatientRace.json"));
        p.setTelephone(readFileData("PatientTelephone.json"));
        p.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        p.setChgAuthNested(readFileData("PatientChgAuthUser.json"));
        p.setEntityData(readFileData("PatientEntityData.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<Object>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix(),
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
                p.getBirthCountry(),
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneHome(),
                p.getPhoneExtHome(),
                p.getPhoneCell(),
                p.getRaceCd(),
                p.getRaceCategory(),
                p.getRaceDesc(),
                p.getAddedBy(),
                p.getLastChangedBy(),
                p.getSsn());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
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
                "313431144414");
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientNameTransformationTest() {

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setName(readFileData("PatientName.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
        // Expected
        List<String> expected = Arrays.asList(
                "Singgh",
                "Js",
                "Suurma",
                "Jr");
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientAddressTransformationTest() {

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setAddress(readFileData("PatientAddress.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(

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
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
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
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientRaceTransformationTest() {
        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setRace(readFileData("PatientRace.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getRaceCd(),
                p.getRaceCategory(),
                p.getRaceDesc());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
        // Expected
        List<String> expected = Arrays.asList(
                "2028-9",
                "2028-9",
                "Amer Indian");
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientTelephoneTransformationTest() {

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setTelephone(readFileData("PatientTelephone.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneHome(),
                p.getPhoneExtHome(),
                p.getPhoneCell());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
        // Expected
        List<String> expected = Arrays.asList(
                "2323222422",
                "232",
                "4562323222",
                "211",
                "2823252423");
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientAddChangeAuthUserTransformationTest() {

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setAddAuthNested(readFileData("PersonAddAuthUser.json"));
        p.setChgAuthNested(readFileData("PatientChgAuthUser.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<Long>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getAddedBy(),
                p.getLastChangedBy());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
        // Expected
        List<Long> expected = Arrays.asList(
                10000000L,
                470200741L);
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
    }

    @Test
    public void PatientSsnTransformationTest() {

        // Build the Patient object with the json serialized data
        PersonOp p = new PersonOp();
        p.setEntityData(readFileData("PatientEntityData.json"));

        // Patient Fields to be processed
        Function<PersonOp, List<String>> pDetailsFn = (PatientOp) -> Collections.singletonList(p.getSsn());
        // Assert all fields null before processing
        pDetailsFn.apply(p).forEach(Assertions::assertNull);
        // Process the respective field json to patient fields
        p.processPatient();
        // Expected
        List<String> expected = List.of("313431144414");
        // Validate the patient field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(p));
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
