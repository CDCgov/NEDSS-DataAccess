package gov.cdc.etldatapipeline.person;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class TestUtils {

    public static String readFileData(String fileName) {
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
