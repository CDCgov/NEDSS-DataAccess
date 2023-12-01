package gov.cdc.etldatapipeline;

import gov.cdc.etldatapipeline.changedata.model.NbsPage;
import gov.cdc.etldatapipeline.changedata.model.NbsPageId;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

@SpringBootTest

public class JsonParserTests {

    @Test
    public void parseDebeziumPrimaryKeyNodeTest() throws IOException {
        File file = new File("src/test/resources/NbsPageId81913.json");
        String testData = FileUtils.readFileToString(file,
                Charset.defaultCharset());
        NbsPageId pageId = UtilHelper.getInstance().parseJsonNode(testData,
                "/payload", NbsPageId.class);
        Assertions.assertEquals(81913, pageId.getNbs_page_uid());
    }

    @Test
    public void parseDebeziumValueTest() throws IOException {
        File file = new File("src/test/resources/OdseNbsPageUpdate.json");
        String testData = FileUtils.readFileToString(file,
                Charset.defaultCharset());
        NbsPage page = UtilHelper.getInstance().parseJsonNode(testData,
                "/payload/after", NbsPage.class);
        Assertions.assertEquals(81913, page.getNbs_page_uid());
    }
}
