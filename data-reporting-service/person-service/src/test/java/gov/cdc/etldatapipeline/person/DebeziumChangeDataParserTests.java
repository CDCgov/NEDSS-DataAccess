package gov.cdc.etldatapipeline.person;

import gov.cdc.etldatapipeline.person.model.odse.Person;
import gov.cdc.etldatapipeline.person.utils.UtilHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static gov.cdc.etldatapipeline.person.TestUtils.readFileData;

public class DebeziumChangeDataParserTests {

    @Test
    public void parseDebeziumPrimaryKeyNodeTest() {
        Person personId = UtilHelper.getInstance().parseJsonNode(
                readFileData("rawDataFiles/person/Person.json"),
                "/payload",
                Person.class);
        Assertions.assertEquals("10000001", personId.getPersonUid());
    }

    @Test
    public void parseDebeziumValueTest() {
        Person person = UtilHelper.getInstance().deserializePayload(
                readFileData("rawDataFiles/person/PersonPatientChangeData.json"),
                "/payload/after",
                Person.class);
        Assertions.assertEquals("9005400", person.getPersonUid());
        Assertions.assertEquals(1708702633619L, person.getTs_ms());
        Assertions.assertEquals("u", person.getOp());
    }
}
