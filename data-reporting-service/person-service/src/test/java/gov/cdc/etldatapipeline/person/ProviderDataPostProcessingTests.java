package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.dto.provider.Provider;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderReporting;
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

public class ProviderDataPostProcessingTests {
    @Test
    public void consolidatedProviderTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Provider p = new Provider();
        p.setNameNested(readFileData("PersonName.json"));
        p.setAddressNested(readFileData("PersonAddress.json"));
        p.setTelephoneNested(readFileData("PersonTelephone.json"));
        p.setEntityDataNested(readFileData("PersonEntityData.json"));
        p.setEmailNested(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<Object>> pDetailsFn = (pf) -> Arrays.asList(
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
                pf.getCountry(),
                pf.getPhoneWork(),
                pf.getPhoneExtWork(),
                pf.getPhoneCell(),
                pf.getProviderQuickCode(),
                pf.getProviderRegistrationNum(),
                pf.getProviderRegistrationNumAuth(),
                pf.getEmailWork());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = p.processProvider();
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
                "2323222422",
                "232",
                "2823252423",
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
        Provider personOp = new Provider();
        personOp.setNameNested(readFileData("PersonName.json"));

        // PatientProviderProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastName(),
                p.getMiddleName(),
                p.getFirstName(),
                p.getNameSuffix());
        // Process the respective field json to PatientProviderProvider fields
        ProviderReporting pf = personOp.processProvider();
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
        Provider perOp = new Provider();
        perOp.setAddressNested(readFileData("PersonAddress.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getStreetAddress1(),
                p.getStreetAddress2(),
                p.getCity(),
                p.getZip(),
                p.getCountyCode(),
                p.getCounty(),
                p.getStateCode(),
                p.getState(),
                p.getCountry());
        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = perOp.processProvider();
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
                "840");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderTelephoneTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Provider personOp = new Provider();
        personOp.setTelephoneNested(readFileData("PersonTelephone.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneCell());
        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProvider();
        // Expected
        List<String> expected = Arrays.asList(
                "2323222422",
                "232",
                "2823252423");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderEntityDataTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Provider personOp = new Provider();
        personOp.setEntityDataNested(readFileData("PersonEntityData.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getProviderQuickCode(),
                p.getProviderRegistrationNum(),
                p.getProviderRegistrationNumAuth());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProvider();
        // Expected
        List<String> expected = List.of(
                "12314286",
                "86741517517",
                "3.16.740.1.113883.3.1147.1.1002");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }

    @Test
    public void PatientProviderEmailTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Provider personOp = new Provider();
        personOp.setEmailNested(readFileData("PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmailWork());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProvider();
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
