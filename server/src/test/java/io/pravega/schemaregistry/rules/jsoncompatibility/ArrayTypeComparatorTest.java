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
    }
}
