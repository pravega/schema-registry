package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JsonCompatibilityCheckerTest {

    @Test
    public void testEqualCase() {
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
        //CanBeRead
        Assert.assertTrue(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst));
    }

    @Test
    public void testDependencies() {
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
                "},\n" +
                "\"name\": {\n" +
                "\"properties\": {\n" +
                "\"salutation\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"salutation\"]\n" +
                "}\n" +
                "}\n" +
                "}\n";
        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x.getBytes()),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(y.getBytes()),
                ImmutableMap.of());
        List<SchemaInfo> toValidateAgainst = new ArrayList<>();
        toValidateAgainst.add(toValidate);
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainst));
        // canBeRead
        Assert.assertTrue(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst));
        toValidateAgainst.add(schemaInfo1);
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainst));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst));
        String z = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"credit_card\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"name\"],\n" +
                "\"dependencies\": {\n" +
                "\"credit_card\": {\n" +
                "\"properties\": {\n" +
                "\"billing_address\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"required\": [\"billing_address\"]\n" +
                "}\n" +
                "}\n" +
                "}\n";
        SchemaInfo schemaInfo11 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(z.getBytes()), ImmutableMap.of());
        toValidateAgainst.add(schemaInfo11);
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainst));
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
        toValidateAgainstList.add(toValidateAgainst);
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertTrue(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
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
        toValidateAgainstList.add(toValidateAgainst1);
        Assert.assertFalse(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
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
        toValidateAgainstList.add(toValidateAgainst2);
        Assert.assertFalse(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
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
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertTrue(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
        //remove required array element
        String x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\", \"email\", \"address\"]\n" +
                "}\n";
        SchemaInfo toValidateAgainst1 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        toValidateAgainstList.add(toValidateAgainst1);
        Assert.assertTrue(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
        x2 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"required\": [\"name\"]\n" +
                "}\n";
        SchemaInfo toValidateAgainst2 = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        toValidateAgainstList.clear();
        toValidateAgainstList.add(toValidateAgainst2);
        Assert.assertFalse(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertTrue(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
    }
    
    @Test
    public void testDynamicProperties() {
        JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
        String x1 = "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" }\n" +
                "},\n" +
                "\"additionalProperties\": { \"type\": \"string\" }\n" +
                "}\n";
        String x2 =  "{\n" +
                "\"type\": \"object\",\n" +
                "\"properties\": {\n" +
                "\"name\": { \"type\": \"string\" },\n" +
                "\"email\": { \"type\": \"string\" },\n" +
                "\"address\": { \"type\": \"string\" },\n" +
                "\"telephone\": { \"type\": \"string\" },\n" +
                "\"SSN\": { \"type\": \"number\" }\n" +
                "},\n" +
                "\"additionalProperties\": { \"type\": \"string\" }\n" +
                "}\n";
        SchemaInfo toValidate = new SchemaInfo("toValidate", SerializationFormat.Json, ByteBuffer.wrap(x1.getBytes()),
                ImmutableMap.of());
        SchemaInfo toValidateAgainst = new SchemaInfo("toValidateAgainst", SerializationFormat.Json,
                ByteBuffer.wrap(x2.getBytes()), ImmutableMap.of());
        List<SchemaInfo> toValidateAgainstList = new ArrayList<>();
        toValidateAgainstList.add(toValidateAgainst);
        Assert.assertFalse(jsonCompatibilityChecker.canRead(toValidate, toValidateAgainstList));
        Assert.assertFalse(jsonCompatibilityChecker.canBeRead(toValidate, toValidateAgainstList));
    }
}
	