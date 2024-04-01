package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.dto.provider.Provider;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderReporting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static gov.cdc.etldatapipeline.person.TestUtils.readFileData;

public class ProviderDataPostProcessingTests {
    private static final String FILE_PREFIX = "rawDataFiles/person/";

    @Test
    public void consolidatedProviderTransformationTest() {

        // Build the PatientProvider object with the json serialized data
        Provider p = new Provider();
        p.setNameNested(readFileData(FILE_PREFIX + "PersonName.json"));
        p.setAddressNested(readFileData(FILE_PREFIX + "PersonAddress.json"));
        p.setTelephoneNested(readFileData(FILE_PREFIX + "PersonTelephone.json"));
        p.setEntityDataNested(readFileData(FILE_PREFIX + "PersonEntityData.json"));
        p.setEmailNested(readFileData(FILE_PREFIX + "PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<Object>> pDetailsFn = (pf) -> Arrays.asList(
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
                pf.getPhoneWork(),
                pf.getPhoneExtWork(),
                pf.getPhoneCell(),
                pf.getProviderQuickCode(),
                pf.getProviderRegistrationNum(),
                pf.getProviderRegistrationNumAuth(),
                pf.getEmailWork());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = p.processProviderReporting();
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
        personOp.setNameNested(readFileData(FILE_PREFIX + "PersonName.json"));

        // PatientProviderProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getLastNm(),
                p.getMiddleNm(),
                p.getFirstNm(),
                p.getNmSuffix());
        // Process the respective field json to PatientProviderProvider fields
        ProviderReporting pf = personOp.processProviderReporting();
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
        perOp.setAddressNested(readFileData(FILE_PREFIX + "PersonAddress.json"));

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
                p.getCountryCode());
        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = perOp.processProviderReporting();
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
        personOp.setTelephoneNested(readFileData(FILE_PREFIX + "PersonTelephone.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getPhoneWork(),
                p.getPhoneExtWork(),
                p.getPhoneCell());
        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProviderReporting();
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
        personOp.setEntityDataNested(readFileData(FILE_PREFIX + "PersonEntityData.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Arrays.asList(
                p.getProviderQuickCode(),
                p.getProviderRegistrationNum(),
                p.getProviderRegistrationNumAuth());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProviderReporting();
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
        personOp.setEmailNested(readFileData(FILE_PREFIX + "PersonEmail.json"));

        // PatientProvider Fields to be processed
        Function<ProviderReporting, List<String>> pDetailsFn = (p) -> Collections.singletonList(p.getEmailWork());

        // Process the respective field json to PatientProvider fields
        ProviderReporting pf = personOp.processProviderReporting();
        // Expected
        List<String> expected = List.of("someone2@email.com");
        // Validate the PatientProvider field processing
        Assertions.assertEquals(expected, pDetailsFn.apply(pf));
    }
}
