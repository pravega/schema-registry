package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class SubSchemaComparator {
    JsonCompatibilityChecker jsonCompatibilityChecker;
    
    public void setJsonCompatibilityChecker() {
        this.jsonCompatibilityChecker = new JsonCompatibilityChecker();
    }
    
    public BreakingChanges checkSubSchemas(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.has("anyOf")) {
            if(toCheckAgainst.has("oneOf") || toCheckAgainst.has("allOf"))
                return BreakingChanges.SUBSCHEMA_TYPE_CHANGED;
            else {
                if(toCheckAgainst.has("anyOf"))
                    return nonAllOfComparator(toCheck, toCheckAgainst);
                else
                    return BreakingChanges.SUBSCHEMA_TYPE_ADDED;
            }
        }
        else if(toCheck.has("oneOf")) {
            if(toCheckAgainst.has("anyOf") || toCheckAgainst.has("allOf"))
                return BreakingChanges.SUBSCHEMA_TYPE_CHANGED;
            else {
                if(toCheckAgainst.has("oneOf"))
                    return nonAllOfComparator(toCheck, toCheckAgainst);
                else
                    return BreakingChanges.SUBSCHEMA_TYPE_ADDED;
            }
        }
        else if(toCheck.has("allOf")) {
            if(toCheckAgainst.has("anyOf") || toCheckAgainst.has("oneOf"))
                return BreakingChanges.SUBSCHEMA_TYPE_CHANGED;
            else {
                if(toCheckAgainst.has("allOf"))
                    return allOfComparator(toCheck, toCheckAgainst);
                else
                    return BreakingChanges.SUBSCHEMA_TYPE_ADDED;
            }
        }
        return null;
    }
    
    private BreakingChanges nonAllOfComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.has("oneOf")) {
            ArrayNode toCheckArray = (ArrayNode) toCheck.get("oneOf");
            ArrayNode toCheckAgainstArray = (ArrayNode) toCheckAgainst.get("oneOf");
            if(toCheckArray.size() < toCheckAgainstArray.size())
                return BreakingChanges.SUBSCHEMA_TYPE_ONE_OF_SCHEMAS_DECREASED;
            else {
                if (!nonAllOfCompatibilityComputation(toCheckArray, toCheckAgainstArray))
                    return BreakingChanges.SUBSCHEMA_TYPE_ONE_OF_SCHEMAS_CHANGED;
            }
        }
        else {
            ArrayNode toCheckArray = (ArrayNode) toCheck.get("anyOf");
            ArrayNode toCheckAgainstArray = (ArrayNode) toCheckAgainst.get("anyOf");
            if(toCheckArray.size() < toCheckAgainstArray.size())
                return BreakingChanges.SUBSCHEMA_TYPE_ANY_OF_SCHEMAS_DECREASED;
            else {
                if (!nonAllOfCompatibilityComputation(toCheckArray, toCheckAgainstArray))
                    return BreakingChanges.SUBSCHEMA_TYPE_ANYOF_SCHEMAS_CHANGED;
            }
        }
        return null;
    }
    
    private BreakingChanges allOfComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayNode toCheckArray = (ArrayNode) toCheck.get("allOf");
        ArrayNode toCheckAgainstArray = (ArrayNode) toCheckAgainst.get("allOf");
        if(!(toCheckArray.size() <= toCheckAgainstArray.size()))
            return BreakingChanges.SUBSCHEMA_TYPE_ALL_OF_SCHEMAS_INCREASED;
        else {
            if (!allOfCompatibilityComputation(toCheckArray, toCheckAgainstArray))
                return BreakingChanges.SUBSCHEMA_TYPE_ALL_OF_SCHEMAS_CHANGED;
        }
        return null;
    }
    
    private boolean allOfCompatibilityComputation(ArrayNode toCheckArray, ArrayNode toCheckAgainstArray) {
        for(int i=0;i<toCheckArray.size();i++) {
            int flag = 0;
            for(int j=0;j<toCheckAgainstArray.size();j++) {
                if(jsonCompatibilityChecker.checkNodeType(toCheckArray.get(i), toCheckAgainstArray.get(j))==null) {
                    flag=1;
                    break;
                }
            }
            if(flag==0)
                return false;
        }
        return true;
    }
    
    private boolean nonAllOfCompatibilityComputation(ArrayNode toCheckArray, ArrayNode toCheckAgainstArray) {
        for(int i=0;i<toCheckAgainstArray.size();i++) {
            int flag = 0;
            for(int j=0;j<toCheckArray.size();j++) {
                if(jsonCompatibilityChecker.checkNodeType(toCheckAgainstArray.get(i), toCheckArray.get(j))==null) {
                    flag=1;
                    break;
                }
            }
            if(flag==0)
                return false;
        }
        return true;
    }
}
