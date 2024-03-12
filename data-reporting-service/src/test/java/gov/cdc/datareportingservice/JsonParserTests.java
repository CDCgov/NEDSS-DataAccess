//package gov.cdc.datareportingservice;
//
//import gov.cdc.datareportingservice.changedata.model.odse.Person;
//import gov.cdc.datareportingservice.changedata.utils.UtilHelper;
//import org.apache.commons.io.FileUtils;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//import org.springframework.boot.test.context.SpringBootTest;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.charset.Charset;
//
//@SpringBootTest
//public class JsonParserTests {
//
//    @Test
//    public void parseDebeziumPrimaryKeyNodeTest() throws IOException {
//        File file = new File("src/test/resources/Person.json");
//        String testData = FileUtils.readFileToString(file,
//                Charset.defaultCharset());
//        Person personId = UtilHelper.getInstance().parseJsonNode(testData,
//                "/payload", Person.class);
//        Assertions.assertEquals("10000001", personId.getPersonUid());
//    }
//
//    @Test
//    public void parseDebeziumValueTest() throws IOException {
//        File file = new File("src/test/resources/Person_ChangeData.json");
//        String testData = FileUtils.readFileToString(file,
//                Charset.defaultCharset());
//        Person person = UtilHelper.getInstance().deserializePayload(testData,
//                "/payload/after", Person.class);
//        Assertions.assertEquals("9005400", person.getPersonUid());
//        Assertions.assertEquals(1708702633619L, person.getTs_ms());
//        Assertions.assertEquals("u", person.getOp());
//    }
//}
