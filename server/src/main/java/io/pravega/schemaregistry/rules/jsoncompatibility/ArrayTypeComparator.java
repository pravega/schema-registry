package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterators;

import java.util.Iterator;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.BreakingChanges;

public class ArrayTypeComparator {
    JsonCompatibilityChecker jsonCompatibilityChecker = new JsonCompatibilityChecker();
    public BreakingChanges compareArrays(JsonNode toCheck, JsonNode toCheckAgainst) {
        if (toCheck.isArray() && toCheckAgainst.isArray())
            return arraySimpleBodyComparision(toCheck, toCheckAgainst);
        else {
            if (checkUniqueItems(toCheck, toCheckAgainst) != null)
                return checkUniqueItems(toCheck, toCheckAgainst);
            BreakingChanges minMaxChanges = minMaxItems(toCheck, toCheckAgainst);
            if (minMaxChanges != null)
                return minMaxChanges;
            BreakingChanges additionalItemsChanges = additionalItems(toCheck, toCheckAgainst);
            if (additionalItemsChanges != null)
                return additionalItemsChanges;
        }
        return null;
    }

    private BreakingChanges arraySimpleBodyComparision(JsonNode toCheck, JsonNode toCheckAgainst) {
        Iterator<String> allNodes = Iterators.concat(toCheck.fieldNames(), toCheckAgainst.fieldNames());
        while(allNodes.hasNext()) {
            String item = allNodes.next();
            if(!toCheck.has(item))
                return BreakingChanges.ARRAY_SIMPLE_BODY_CHECK_ELEMENT_REMOVED;
            else if (!toCheckAgainst.has(item))
                return BreakingChanges.ARRAY_SIMPLE_BODY_CHECK_ELEMENT_ADDED;
        }
        return null;
    }

    private BreakingChanges checkUniqueItems(JsonNode toCheck, JsonNode toCheckAgainst) {
        if (toCheck.get("uniqueItems").isBoolean() && toCheck.get("uniqueItems").asText() == "true") {
            if (toCheckAgainst.get("uniqueItems") == null)
                return BreakingChanges.ARRAY_UNIQUE_ITEMS_CONDITION_ENABLED;
        }
        return null;
    }

    private BreakingChanges minMaxItems(JsonNode toCheck, JsonNode toCheckAgainst) {
        if (toCheck.get("maxItems") != null && toCheckAgainst.get("maxItems") == null)
            return BreakingChanges.ARRAY_MAX_ITEMS_CONDITION_ADDED;
        else if (toCheck.get("maxItems") != null && toCheckAgainst.get("maxItems") != null) {
            int originalMaxLimit = toCheckAgainst.get("maxItems").asInt();
            int changedMaxLimit = toCheck.get("maxItems").asInt();
            if (changedMaxLimit < originalMaxLimit)
                return BreakingChanges.ARRAY_MAX_ITEMS_VALUE_DECREASED;
        }
        if (toCheck.get("minItems") != null && toCheckAgainst.get("minItems") == null)
            return BreakingChanges.ARRAY_MIN_ITEMS_CONDITION_ADDED;
        else if (toCheck.get("minItems") != null && toCheckAgainst.get("minItems") != null) {
            int originalMinLimit = toCheckAgainst.get("minItems").asInt();
            int changedMinLimit = toCheck.get("minItems").asInt();
            if (changedMinLimit > originalMinLimit)
                return BreakingChanges.ARRAY_MIN_ITEMS_VALUE_INCREASED;
        }
        return null;
    }

    private BreakingChanges additionalItems(JsonNode toCheck, JsonNode toCheckAgainst) {
        if (toCheck.get("additionalItems") != null && toCheckAgainst.get("additionalItems") == null) {
            if (toCheck.get("additionalItems").isBoolean() && toCheck.get("additionalItems").asText() == "false")
                return BreakingChanges.ARRAY_ADDITIONAL_ITEMS_DISABLED;
            else if (toCheck.get("additionalItems").isObject())
                return BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_DECREASED;
        } else if (toCheck.get("additionalItems") != null && toCheckAgainst.get("additionalItems") != null) {
            if (toCheck.get("additionalItems").isBoolean() && toCheck.get("additionalItems").asText() == "false") {
                if(!(toCheckAgainst.get("additionalItems").asText() == "false"))
                    return BreakingChanges.ARRAY_ADDITIONAL_ITEMS_DISABLED;
            }
            else if (toCheck.get("additionalItems").isObject()) {
                if(toCheckAgainst.get("additionalItems").isObject()) {
                    if(jsonCompatibilityChecker.checkNodeType(toCheck.get("additionalItems"), toCheckAgainst.get("additionalItems")) != null)
                        return BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_DECREASED;
                    else if (toCheckAgainst.get("additionalItems").isBoolean() && toCheckAgainst.get("additionalItems").asText() == "true")
                        return BreakingChanges.ARRAY_ADDITIONAL_ITEMS_SCOPE_DECREASED;
                }
            }
        }
        return null;
    }
    
    private boolean isDynamicArray(JsonNode node) {
        if(node.get("additionalItems") == null)
            return true;
        else if(node.get("additionalItems").isBoolean()) {
            if(node.get("additionalItems").asText() == "true")
                return true;
        }
        return false;
    }
    
    private  boolean isDynamicArrayWithCondition(JsonNode node) {
        if(node.get("additionalItems").isObject())
            return true;
        return false;
    }
    
    private boolean isStaticArray(JsonNode node) {
        if(node.get("additionalItems").isBoolean()) {
            if(node.get("additionalItems").asText() == "false")
                return true;
        }
        return false;
    }
}
