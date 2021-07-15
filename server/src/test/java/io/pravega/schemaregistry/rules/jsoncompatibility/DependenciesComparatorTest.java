package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DependenciesComparatorTest {

    @Test
    public void testCheckDependencies() throws IOException {
        DependenciesComparator dependenciesComparator = new DependenciesComparator();
        ObjectMapper objectMapper = new ObjectMapper();
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\"],\n" +
                "\"billing_address\": [\"credit_card\"]\n" +
                "}\n" +
                "}\n";
        String x2 = "{\n" +
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
        JsonNode toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        JsonNode toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DEPENDENCY_ADDED_IN_ARRAY_FORM,
                dependenciesComparator.checkDependencies(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" },\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": [\"billing_address\", \"name\"]\n" +
                "}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DEPENDENCY_ARRAY_ELEMENTS_NON_REMOVAL,
                dependenciesComparator.checkDependencies(toCheck, toCheckAgainst));
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DEPENDENCY_SECTION_ADDED,
                dependenciesComparator.checkDependencies(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": {\n" +
                "\"properties\": {\n" +
                "\"billing_address\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n" +
                "}\n";
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": {\n" +
                "\"properties\": {\n" +
                "\"card_number\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"card_number\"]\n" +
                "}\n" +
                "}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        toCheckAgainst = objectMapper.readTree(ByteBuffer.wrap(x2.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DEPENDENCY_IN_SCHEMA_FORM_MODIFIED,
                dependenciesComparator.checkDependencies(toCheck, toCheckAgainst));
        x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": {\n" +
                "\"properties\": {\n" +
                "\"card_number\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"card_number\"]\n" +
                "},\n" +
                "\"name\": {\n" +
                "\"properties\": {\n" +
                "\"salutation\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"salutation\"]\n" +
                "}\n" +
                "}\n" +
                "}\n";
        toCheck = objectMapper.readTree(ByteBuffer.wrap(x1.getBytes()).array());
        Assert.assertEquals(BreakingChangesStore.BreakingChanges.DEPENDENCY_ADDED_IN_SCHEMA_FORM,
                dependenciesComparator.checkDependencies(toCheck, toCheckAgainst));
    }
}
