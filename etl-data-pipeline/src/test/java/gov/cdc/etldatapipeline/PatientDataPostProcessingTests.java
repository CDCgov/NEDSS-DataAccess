package gov.cdc.etldatapipeline;

import gov.cdc.etldatapipeline.changedata.model.dto.PatientOP;
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
        PatientOP p = new PatientOP();
        p.setPatientName(readFileData("PatientName.json"));
        p.setPatientAddress(readFileData("PatientAddress.json"));
        p.setPatientRace(readFileData("PatientRace.json"));
        p.setPatientTelephone(readFileData("PatientTelephone.json"));
        p.setPatientAddAuthNested(readFileData("PatientAddAuthUser.json"));
        p.setPatientChgAuthNested(readFileData("PatientChgAuthUser.json"));
        p.setPatientEntityData(readFileData("PatientEntityData.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<Object>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPatientLastName(),
                p.getPatientMiddleName(),
                p.getPatientFirstName(),
                p.getPatientNameSuffix(),
                p.getPatientStreetAddress1(),
                p.getPatientStreetAddress2(),
                p.getPatientCity(),
                p.getPatientZip(),
                p.getPatientCountyCode(),
                p.getPatientCounty(),
                p.getPatientStateCode(),
                p.getPatientState(),
                p.getPatientCountryCode(),
                p.getPatientCountry(),
                p.getPatientBirthCountry(),
                p.getPatientPhoneWork(),
                p.getPatientPhoneExtWork(),
                p.getPatientPhoneHome(),
                p.getPatientPhoneExtHome(),
                p.getPatientPhoneCell(),
                p.getPatientRaceCd(),
                p.getPatientRaceCategory(),
                p.getPatientRaceDesc(),
                p.getPatientAddedBy(),
                p.getPatientLastChangedBy(),
                p.getPatientSsn());
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
        PatientOP p = new PatientOP();
        p.setPatientName(readFileData("PatientName.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPatientLastName(),
                p.getPatientMiddleName(),
                p.getPatientFirstName(),
                p.getPatientNameSuffix());
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
        PatientOP p = new PatientOP();
        p.setPatientAddress(readFileData("PatientAddress.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(

                p.getPatientStreetAddress1(),
                p.getPatientStreetAddress2(),
                p.getPatientCity(),
                p.getPatientZip(),
                p.getPatientCountyCode(),
                p.getPatientCounty(),
                p.getPatientStateCode(),
                p.getPatientState(),
                p.getPatientCountryCode(),
                p.getPatientCountry(),
                p.getPatientBirthCountry());
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
        PatientOP p = new PatientOP();
        p.setPatientRace(readFileData("PatientRace.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPatientRaceCd(),
                p.getPatientRaceCategory(),
                p.getPatientRaceDesc());
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
        PatientOP p = new PatientOP();
        p.setPatientTelephone(readFileData("PatientTelephone.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<String>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPatientPhoneWork(),
                p.getPatientPhoneExtWork(),
                p.getPatientPhoneHome(),
                p.getPatientPhoneExtHome(),
                p.getPatientPhoneCell());
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
        PatientOP p = new PatientOP();
        p.setPatientAddAuthNested(readFileData("PatientAddAuthUser.json"));
        p.setPatientChgAuthNested(readFileData("PatientChgAuthUser.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<Long>> pDetailsFn = (PatientOp) -> Arrays.asList(
                p.getPatientAddedBy(),
                p.getPatientLastChangedBy());
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
        PatientOP p = new PatientOP();
        p.setPatientEntityData(readFileData("PatientEntityData.json"));

        // Patient Fields to be processed
        Function<PatientOP, List<String>> pDetailsFn = (PatientOp) -> Collections.singletonList(p.getPatientSsn());
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
