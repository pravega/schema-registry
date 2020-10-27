package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
    
    @Test
    public void testHasDynamicPropertySet() throws IOException {
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
        Assert.assertTrue(jsonCompatibilityCheckerUtils.hasDynamicPropertySet(node));
        x = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"additionalProperties\" : true,\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n";
        node = objectMapper.readTree(ByteBuffer.wrap(x.getBytes()).array());
        Assert.assertTrue(jsonCompatibilityCheckerUtils.hasDynamicPropertySet(node));
    }
    
    @Test
    public void testHasStaticPropertySet() throws IOException {
        String x = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"additionalProperties\" : false,\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n";
        JsonNode node = objectMapper.readTree(ByteBuffer.wrap(x.getBytes()).array());
        Assert.assertFalse(jsonCompatibilityCheckerUtils.hasDynamicPropertySet(node));
    }
    
    @Test
    public void testIsInRequired() throws IOException {
        String toSearch = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\", \"credit_card\"],\n" +
                "\"additionalProperties\" : false,\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n";
        JsonNode node = objectMapper.readTree(ByteBuffer.wrap(toSearch.getBytes()).array());
        Assert.assertTrue(jsonCompatibilityCheckerUtils.isInRequired("name", node));
    }
    
    @Test
    public void testArrayComparisionOnlyRemoval() throws IOException {
        String arrayFinal = "[\"item1\", \"item2\"]";
        String arrayOriginal = "[\"item1\", \"item2\", \"item3\"]";
        JsonNode finalNode = objectMapper.readTree(ByteBuffer.wrap(arrayFinal.getBytes()).array());
        ArrayNode finalArray =(ArrayNode) finalNode;
        JsonNode originalNode = objectMapper.readTree(ByteBuffer.wrap(arrayOriginal.getBytes()).array());
        ArrayNode originalArray =(ArrayNode) originalNode;
        Assert.assertTrue(jsonCompatibilityCheckerUtils.arrayComparisionOnlyRemoval(finalArray, originalArray));
    }
    
    @Test
    public void testArrayComparisionOnlyAddition() throws IOException {
        String arrayOriginal = "[\"item1\", \"item2\"]";
        String arrayFinal = "[\"item1\", \"item2\", \"item3\"]";
        JsonNode finalNode = objectMapper.readTree(ByteBuffer.wrap(arrayFinal.getBytes()).array());
        ArrayNode finalArray =(ArrayNode) finalNode;
        JsonNode originalNode = objectMapper.readTree(ByteBuffer.wrap(arrayOriginal.getBytes()).array());
        ArrayNode originalArray =(ArrayNode) originalNode;
        Assert.assertTrue(jsonCompatibilityCheckerUtils.arrayComparisionOnlyAddition(finalArray, originalArray));
    }
}
