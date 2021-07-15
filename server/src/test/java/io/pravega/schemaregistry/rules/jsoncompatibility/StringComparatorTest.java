package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StringComparatorTest {
    
    @Test
    public void testMinLengthComparator() throws IOException {
        StringComparator stringComparator = new StringComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"string\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"minLength\": 3\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_MIN_LENGTH_ADDED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"minLength\": 2\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_MIN_LENGTH_VALUE_INCREASED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testMaxLengthComparator() throws IOException {
        StringComparator stringComparator = new StringComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"string\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"maxLength\": 3\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_MAX_LENGTH_ADDED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"maxLength\": 4\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_MAX_LENGTH_VALUE_DECREASED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
    }
    
    @Test
    public void testPatternComparator() throws IOException {
        StringComparator stringComparator = new StringComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x2 = "{\n" +
                "\"type\": \"string\"\n" +
                "}\n";
        String x1 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"pattern\": \"^(\\\\([0-9]{3}\\\\))?[0-9]{3}-[0-9]{4}$\"\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_PATTERN_ADDED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"pattern\": \"^(\\\\([0-9]{3}\\\\))?[0-9]{4}-[0-9]{5}$\"\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.STRING_TYPE_PATTERN_MODIFIED,
                stringComparator.stringComparator(toCheck, toCheckAgainst));
    }
}
