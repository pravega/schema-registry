package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class JsonCompatibilityCheckerTest {

    @Test
    public void testCanRead() {
        JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
        String x = "{\n" +
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
        String y = "{\n" +
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
        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x.getBytes()),
                ImmutableMap.of());
        List<SchemaInfo> toValidateAgainst = new ArrayList<>();
        SchemaInfo schemaInfo1 = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(y.getBytes()),
                ImmutableMap.of());
        toValidateAgainst.add(schemaInfo1);
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainst));
    }

    @Test
    public void testPrintNodes() throws IOException {
        JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
        String x = "{\n" +
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
        String y = "{\n" +
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
        String z = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"first_name\": { \"type\": \"string\" },\n" +
                "\"last_name\": { \"type\": \"string\" },\n" +
                "\"birthday\": { \"type\": \"string\", \"format\": \"date\" },\n" +
                "\"address\": {\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"street_address\": { \"type\": \"string\" },\n" +
                "\"city\": { \"type\": \"string\" },\n" +
                "\"state\": { \"type\": \"string\" },\n" +
                "\"country\": { \"type\" : \"string\" }\n" +
                "},\n" +
                "\"required\": [\"city\"]\n" +
                "}\n" +
                "},\n" +
                "\"required\": [\"first_name\"]\n" +
                "}\n";
        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x.getBytes()),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(y.getBytes()),
                ImmutableMap.of());
        SchemaInfo schemaInfo11 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(z.getBytes()), ImmutableMap.of());
        List<SchemaInfo> toValidateAgainst = new ArrayList<>();
        toValidateAgainst.add(schemaInfo1);
        toValidateAgainst.add(schemaInfo11);
        jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst);
    }

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

    @Test
    public void testBasicProperties() throws IOException {
        JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
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
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";

        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x1.getBytes()),
                ImmutableMap.of());
        SchemaInfo toValidateAgainst = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        List<SchemaInfo> toValidateAgainstList = new ArrayList<>();

        //different properties
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name1\": { \"type\": \"string\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        SchemaInfo toValidateAgainst1 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());

        //different property values 
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"number\": { \"type\": \"number\" },\n" +
                "\"street_name\": { \"type\": \"number\" },\n" +
                "\"street_type\": { \"type\": \"string\"}\n" +
                "}\n" +
                "}\n";
        SchemaInfo toValidateAgainst2 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        toValidateAgainstList.add(toValidateAgainst);
        toValidateAgainstList.add(toValidateAgainst1);
        toValidateAgainstList.add(toValidateAgainst2);
        jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList);
    }

    @Test
    public void testRequired() throws IOException {
        JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
        // equal test
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\", \"email\"]\n" +
                "}\n";
        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x1.getBytes()),
                ImmutableMap.of());
        SchemaInfo toValidateAgainst = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x1.getBytes()), ImmutableMap.of());
        List<SchemaInfo> toValidateAgainstList = new ArrayList<>();
        toValidateAgainstList.add(toValidateAgainst);
        //remove required array element
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"email\"]\n" +
                "}\n";
        SchemaInfo toValidateAgainst1 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        toValidateAgainstList.add(toValidateAgainst1);
    }
}
	