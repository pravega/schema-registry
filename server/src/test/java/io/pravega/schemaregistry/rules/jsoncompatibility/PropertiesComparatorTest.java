package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PropertiesComparatorTest {
    @Test
    public void testBasicProperties() throws IOException {
        PropertiesComparator propertiesComparator = new PropertiesComparator();
        propertiesComparator.setJsonCompatibilityChecker();
        ObjectMapper objectMapper = new ObjectMapper();
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.PROPERTY_ADDED_TO_DYNAMIC_PROPERTY_SET,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": false\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"required\": [\"city\"]\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"number\"}\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"number\"}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.PROPERTY_ADDED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": false\n" +
                "}\n";
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.PROPERTY_REMOVED_FROM_STATIC_PROPERTY_SET,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"number\"}\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"number\"}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.PROPERTY_REMOVED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testMinMaxProperties() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        PropertiesComparator propertiesComparator = new PropertiesComparator();
        propertiesComparator.setJsonCompatibilityChecker();
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"minProperties\": 1\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.MIN_PROPERTIES_ADDED,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"minProperties\": 1\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"minProperties\": 2\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.MIN_PROPERTIES_LIMIT_INCREASED,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"maxProperties\": 3\n" +
                "}\n";
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.MAX_PROPERTIES_ADDED,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"maxProperties\": 4\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"maxProperties\": 3\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.MAX_PROPERTIES_LIMIT_DECREASED,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testAdditionalProperties() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        PropertiesComparator propertiesComparator = new PropertiesComparator();
        propertiesComparator.setJsonCompatibilityChecker();
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": false\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ADDITIONAL_PROPERTIES_REMOVED,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"string\"}\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"additionalProperties\": {\"type\": \"number\"}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ADDITIONAL_PROPERTIES_NON_COMPATIBLE_CHANGE,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testRequiredProperties() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        PropertiesComparator propertiesComparator = new PropertiesComparator();
        propertiesComparator.setJsonCompatibilityChecker();
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"required\": [\"city\"]\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"required\": [\"city\"]\n" +
                "}\n";
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"},\n" +
                "\"city\": { \"type\": \"string\"}\n" +
                "},\n" +
                "\"required\": [\"city\", \"number\"]\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT,
                propertiesComparator.checkProperties(toCheck, toCheckAgainst));
    }
}
