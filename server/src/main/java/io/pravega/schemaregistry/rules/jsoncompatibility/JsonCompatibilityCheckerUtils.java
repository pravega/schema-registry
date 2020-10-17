package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class JsonCompatibilityCheckerUtils {
    
    public String getTypeValue(JsonNode node) {
        String value = null;
        while(node.fieldNames().hasNext()) {
            if (node.fieldNames().next().equals("type"))
                value = node.get("type").textValue();
        }
        return value;
    }
    
    public boolean hasDynamicPropertySet(JsonNode node) {
        if(node.get("additionalProperties")==null && node.get("patternProperties")==null)
            return true;
        return false;
    }
    
    public boolean hasStaticPropertySet(JsonNode node) {
        if(node.get("patternProperties") == null && node.get("additionalProperties").asText() == "false")
            return true;
        return false;
    }
    
    public boolean isInRequired(String toSearch, JsonNode toSearchIn) {
        if(toSearchIn.get("required") != null) {
            ArrayNode arrayToSearch = (ArrayNode) toSearchIn.get("required");
            if(arrayToSearch.has(toSearch))
                return true;
        }
        return false;
    }
}
