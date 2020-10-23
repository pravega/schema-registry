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
    
    public boolean arrayComparisionOnlyRemoval(ArrayNode toCheck, ArrayNode toCheckAgainst) {
        // every element in toCheck array must be in toCheckAgainst
        for(int i=0;i<toCheck.size();i++) {
            if(!toCheckAgainst.has(toCheck.get(i).asText()))
                return false;
        }
        return true;
    }
    
    public boolean arrayComparisionOnlyAddition(ArrayNode toCheck, ArrayNode toCheckAgainst) {
        // every element in toCheckAgainst array must be in toCheck.
        for(int i=0;i<toCheckAgainst.size();i++) {
            if(!toCheck.has(toCheckAgainst.get(i).asText()))
                return false;
        }
        return true;
    }
}
