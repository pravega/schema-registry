package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JsonCompatibilityCheckerUtilsTest {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();
    @Test
    public void testGetTypeValue() throws IOException {
        String x = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n";
        JsonNode node = objectMapper.readTree(ByteBuffer.wrap(x.getBytes()).array());
        Assert.assertEquals("object", jsonCompatibilityCheckerUtils.getTypeValue(node));
    }
}
