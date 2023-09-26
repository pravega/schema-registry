package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ArrayTypeComparatorTest {

    @Test
    public void testCheckUniqueItems() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayTypeComparator arrayTypeComparator = new ArrayTypeComparator();
        arrayTypeComparator.setJsonTypeComparator();
        String x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"uniqueItems\": true\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"uniqueItems\": false\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_UNIQUE_ITEMS_CONDITION_ENABLED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"array\"\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_UNIQUE_ITEMS_CONDITION_ENABLED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
    }

    @Test
    public void testMinMaxItems() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayTypeComparator arrayTypeComparator = new ArrayTypeComparator();
        arrayTypeComparator.setJsonTypeComparator();
        String x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"maxItems\": 3\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"array\"\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_MAX_ITEMS_CONDITION_ADDED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"maxItems\": 4\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_MAX_ITEMS_VALUE_DECREASED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"minItems\": 3\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"array\"\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_MIN_ITEMS_CONDITION_ADDED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"minItems\": 2\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_MIN_ITEMS_VALUE_INCREASED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
    }

    @Test
    public void testAdditionalItems() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayTypeComparator arrayTypeComparator = new ArrayTypeComparator();
        arrayTypeComparator.setJsonTypeComparator();
        String x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": false\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"array\"\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_ADDITIONAL_ITEMS_DISABLED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": { \"type\": \"string\" }\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_DECREASED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": true\n" +
                "}\n";
        x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": false\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_ADDITIONAL_ITEMS_DISABLED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": { \"type\": \"string\" }\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_DECREASED,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"additionalItems\": { \"type\": \"number\" }\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_INCOMPATIBLE_CHANGE,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
    }

    @Test
    public void testItemValidation() throws IOException {
        // node.get(items) is an object
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayTypeComparator arrayTypeComparator = new ArrayTypeComparator();
        arrayTypeComparator.setJsonTypeComparator();
        String x1 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"items\": {\n" +
                "\"type\": \"number\"\n" +
                "}\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"array\" ,\n" +
                "\"items\": {\n" +
                "\"type\": \"string\"\n" +
                "}\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertNotNull(arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DATA_TYPE_MISMATCH,
                arrayTypeComparator.compareArrays(toCheck, toCheckAgainst));
    }
}
