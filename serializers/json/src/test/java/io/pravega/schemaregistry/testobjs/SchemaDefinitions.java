/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.testobjs;

public class SchemaDefinitions {
    public static final String JSON_SCHEMA_STRING = "{" +
            "\"title\": \"Person\", " +
            "\"type\": \"object\", " +
            "\"properties\": { " +
            "\"name\": {" +
            "\"type\": \"string\"" +
            "}," +
            "\"age\": {" +
            "\"type\": \"integer\", \"minimum\": 0" +
            "}" + 
            "}" + 
            "}";

    public static final String JSON_SCHEMA_STRING_DRAFT_4 = "{\n" +
            "   \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n" +
            "   \"title\": \"User\",\n" +
            "   \"id\": \"UserV4\",\n" +
            "   \"type\": \"object\",\n" +
            "\t\n" +
            "   \"properties\": {\n" +
            "\t\n" +
            "      \"id\": {\n" +
            "         \"type\": \"integer\"\n" +
            "      },\n" +
            "\t\t\n" +
            "      \"name\": {\n" +
            "         \"type\": \"string\"\n" +
            "      },\n" +
            "\t\t\n" +
            "      \"age\": {\n" +
            "         \"type\": \"number\",\n" +
            "         \"minimum\": 0,\n" +
            "         \"exclusiveMinimum\": true\n" +
            "      }\n" +
            "   },\n" +
            "\t\n" +
            "   \"required\": [\"id\", \"name\", \"age\"]\n" +
            "}";

    public static final String JSON_SCHEMA_STRING_DRAFT_7 = "{\n" +
            "  \"$id\": \"UserV7\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"title\": \"User\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"firstName\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"lastName\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 0\n" +
            "    }\n" +
            "  }\n" +
            "}";
}
