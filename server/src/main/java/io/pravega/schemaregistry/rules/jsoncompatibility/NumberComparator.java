package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class NumberComparator {
    public BreakingChanges compareNumbers(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(maximumComparator(toCheck, toCheckAgainst) != null)
            return maximumComparator(toCheck, toCheckAgainst);
        if(minimumComparator(toCheck, toCheckAgainst) != null)
            return minimumComparator(toCheck, toCheckAgainst);
        if(multipleOfComparator(toCheck, toCheckAgainst) != null)
            return multipleOfComparator(toCheck, toCheckAgainst);
        if(typeChanged(toCheck, toCheckAgainst) != null)
            return typeChanged(toCheck, toCheckAgainst);
        return null;
        //TO DO - Changes from exclusive max/min to max/min & vice versa.
    }
    
    private BreakingChanges maximumComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("maximum") != null && toCheckAgainst.get("maximum") == null)
            return BreakingChanges.NUMBER_TYPE_MAXIMUM_VALUE_ADDED;
        else if(toCheck.get("maximum") != null && toCheckAgainst.get("maximum") != null) {
            int toCheckAgainstMaximum = toCheckAgainst.get("maximum").intValue();
            int toCheckMaximum = toCheck.get("maximum").intValue();
            if(toCheckMaximum < toCheckAgainstMaximum) {
                return BreakingChanges.NUMBER_TYPE_MAXIMUM_VALUE_DECREASED;
            }
        }
        else if(toCheck.get("exclusiveMaximum") != null && toCheckAgainst.get("exclusiveMaximum") == null) {
            return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_ADDED;
        }
        else if(toCheck.get("exclusiveMaximum") != null && toCheckAgainst.get("exclusiveMaximum") != null) {
            if(toCheckAgainst.get("exclusiveMaximum").isBoolean()) {
                if(toCheckAgainst.get("exclusiveMaximum").asText() == "false" && toCheck.get("exclusiveMaximum").asText() == "true")
                    return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_ADDED;
            }
            else {
                int toCheckAgainstExclusiveMaximum = toCheckAgainst.get("exclusiveMaximum").asInt();
                int toCheckExclusiveMaximum = toCheck.get("exclusiveMaximum").asInt();
                if(toCheckExclusiveMaximum < toCheckAgainstExclusiveMaximum)
                    return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MAXIMUM_VALUE_DECREASED;
            }
        }
        return null;
    }
    
    private BreakingChanges minimumComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("minimum") != null && toCheckAgainst.get("minimum") == null)
            return BreakingChanges.NUMBER_TYPE_MINIMUM_VALUE_ADDED;
        else if(toCheck.get("minimum") != null && toCheckAgainst.get("minimum") != null) {
            int toCheckAgainstMinimum = toCheckAgainst.get("minimum").intValue();
            int toCheckMinimum = toCheck.get("minimum").intValue();
            if(toCheckMinimum > toCheckAgainstMinimum) {
                return BreakingChanges.NUMBER_TYPE_MINIMUM_VALUE_INCREASED;
            }
        }
        else if(toCheck.get("exclusiveMinimum") != null && toCheckAgainst.get("exclusiveMinimum") == null) {
            return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_ADDED;
        }
        else if(toCheck.get("exclusiveMinimum") != null && toCheckAgainst.get("exclusiveMinimum") != null) {
            if(toCheckAgainst.get("exclusiveMinimum").isBoolean()) {
                if(toCheckAgainst.get("exclusiveMinimum").asText() == "false" && toCheck.get("exclusiveMinimum").asText() == "true")
                    return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_ADDED;
            }
            else {
                int toCheckAgainstExclusiveMinimum = toCheckAgainst.get("exclusiveMinimum").asInt();
                int toCheckExclusiveMinimum = toCheck.get("exclusiveMinimum").asInt();
                if(toCheckExclusiveMinimum > toCheckAgainstExclusiveMinimum)
                    return BreakingChanges.NUMBER_TYPE_EXCLUSIVE_MINIMUM_VALUE_INCREASED;
            }
        }
        return null;
    }
    
    private BreakingChanges multipleOfComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("multipleOf") != null && toCheckAgainst.get("multipleOf") == null)
            return BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_ADDED;
        else if(toCheck.get("multipleOf") != null && toCheckAgainst.get("multipleOf") != null) {
            int toCheckMultipleOf = toCheck.get("multipleOf").asInt();
            int toCheckAgainstMultipleOf = toCheckAgainst.get("multipleOf").asInt();
            if(toCheckAgainstMultipleOf != toCheckMultipleOf) {
                if(toCheckMultipleOf%toCheckAgainstMultipleOf == 0)
                    return BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_INCREASED;
                else if (toCheckAgainstMultipleOf%toCheckMultipleOf != 0)
                    return BreakingChanges.NUMBER_TYPE_MULTIPLE_OF_NON_DIVISIBLE_CHANGE;
            }
        }
        return null;
    }
    
    private BreakingChanges typeChanged(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("type").asText().equals("integer") && toCheckAgainst.get("type").asText().equals("number"))
            return BreakingChanges.NUMBER_TYPE_CHANGED_FROM_NUMBER_TO_INTEGER;
        return null;
    }
}
