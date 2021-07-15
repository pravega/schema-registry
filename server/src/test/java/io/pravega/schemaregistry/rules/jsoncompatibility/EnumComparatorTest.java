package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EnumComparatorTest {

    @Test
    public void testEnumComparator() throws IOException {
        EnumComparator enumComparator = new EnumComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x1 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"enum\": [\"red\", \"amber\", \"green\"]\n" +
                "}\n";
        String x2 = "{\n" +
                "\"type\": \"string\"\n" +
                "}\n";
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ENUM_TYPE_ADDED,
                enumComparator.enumComparator(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"string\" ,\n" +
                "\"enum\": [\"red\", \"amber\", \"yellow\"]\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.ENUM_TYPE_ARRAY_CONTENTS_NON_ADDITION_OF_ELEMENTS,
                enumComparator.enumComparator(toCheck, toCheckAgainst));
    }
}
