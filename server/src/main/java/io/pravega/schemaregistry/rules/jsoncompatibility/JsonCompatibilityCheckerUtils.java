package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Iterator;

public class JsonCompatibilityCheckerUtils {
    
    public String getTypeValue(JsonNode node) {
        String value = null;
        Iterator<String> fieldNames = node.fieldNames();
        while(fieldNames.hasNext()) {
            if (node.fieldNames().next().equals("type"))
                value = node.get("type").textValue();
                break;
        }
        return value;
    }
    
    public boolean hasDynamicPropertySet(JsonNode node) {
        if(node.get("additionalProperties")==null && node.get("patternProperties")==null)
            return true;
        else if(node.get("additionalProperties").isBoolean() && node.get("patternProperties") == null) {
            if(node.get("additionalProperties").asText() == "true") {
                return true;
            }
        }
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
            for(int i=0;i<arrayToSearch.size();i++) {
                if(arrayToSearch.get(i).asText().equals(toSearch))
                    return true;
            }
        }
        return false;
    }
    
    public boolean arrayComparisionOnlyRemoval(ArrayNode toCheck, ArrayNode toCheckAgainst) {
        // every element in toCheck array must be in toCheckAgainst
        for(int i=0;i<toCheck.size();i++) {
            int flag =0;
            String toSearch = toCheck.get(i).asText();
            for(int j=0;j<toCheckAgainst.size();j++) {
                if(toSearch.equals(toCheckAgainst.get(i).asText())) {
                    flag = 1;
                    break;
                }
            }
            if(flag==0)
                return false;
        }
        return true;
    }
    
    public boolean arrayComparisionOnlyAddition(ArrayNode toCheck, ArrayNode toCheckAgainst) {
        // every element in toCheckAgainst array must be in toCheck.
        for(int i=0;i<toCheckAgainst.size();i++) {
            int flag =0;
            String toSearch = toCheckAgainst.get(i).asText();
            for(int j=0;j<toCheck.size();j++) {
                if(toSearch.equals(toCheck.get(j).asText())) {
                    flag = 1;
                    break;
                }
            }
            if(flag==0)
                return false;
        }
        return true;
    }
}
