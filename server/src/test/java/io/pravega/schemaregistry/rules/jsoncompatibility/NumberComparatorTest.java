package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NumberComparatorTest {

    @Test
    public void testMaximumComparator() throws IOException {
        NumberComparator numberComparator = new NumberComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"number\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"maximum\": 3\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MAXIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMaximum\": 3\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMaximum\": 4\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_DECREASED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"maximum\": 3\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"maximum\": 4\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MAXIMUM_VALUE_DECREASED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMaximum\": true\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMaximum\": false\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testMinimumComparator() throws IOException {
        NumberComparator numberComparator = new NumberComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"number\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"minimum\": 3\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MINIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMinimum\": 3\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMinimum\": 2\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_INCREASED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"minimum\": 3\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"minimum\": 2\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MINIMUM_VALUE_INCREASED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMinimum\": true\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"exclusiveMinimum\": false\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testMultipleOfComparator() throws IOException {
        NumberComparator numberComparator = new NumberComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"number\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"multipleOf\": 10\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_ADDED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"multipleOf\": 5\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_INCREASED,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"number\" ,\n" +
                "\"multipleOf\": 3\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_NON_DIVISIBLE_CHANGE,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testTypeChanged() throws IOException {
        NumberComparator numberComparator = new NumberComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"number\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"integer\"\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.NUMBER_TYPE_CHANGED_FROM_NUMBER_TO_INTEGER,
                numberComparator.compareNumbers(toCheck, toCheckAgainst));
    }
    
    
}
