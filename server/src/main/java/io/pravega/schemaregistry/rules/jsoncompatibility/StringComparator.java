package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class StringComparator {
    
    public BreakingChanges stringComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(minLengthComparator(toCheck, toCheckAgainst) != null)
            return minLengthComparator(toCheck, toCheckAgainst);
        if(maxLengthComparator(toCheck, toCheckAgainst) != null)
            return maxLengthComparator(toCheck, toCheckAgainst);
        if(patternComparator(toCheck, toCheckAgainst) != null)
            return patternComparator(toCheck, toCheckAgainst);
        return null;
    }
    
    private BreakingChanges minLengthComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("minLength") != null && toCheckAgainst.get("minLength") == null)
            return BreakingChanges.STRING_TYPE_MIN_LENGTH_ADDED;
        else if(toCheck.get("minLength") != null && toCheckAgainst.get("minLength") != null) {
            int toCheckMinLength = toCheck.get("minLength").asInt();
            int toCheckAgainstMinLength = toCheckAgainst.get("minLength").asInt();
            if(toCheckMinLength > toCheckAgainstMinLength)
                return BreakingChanges.STRING_TYPE_MIN_LENGTH_VALUE_INCREASED;
        }
        return null;
    }
    
    private BreakingChanges maxLengthComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("maxLength") != null && toCheckAgainst.get("maxLength") == null)
            return BreakingChanges.STRING_TYPE_MAX_LENGTH_ADDED;
        else if(toCheck.get("maxLength") != null && toCheckAgainst.get("maxLength") != null) {
            int toCheckMaxLength = toCheck.get("maxLength").asInt();
            int toCheckAgainstMaxLength = toCheckAgainst.get("maxLength").asInt();
            if(toCheckMaxLength < toCheckAgainstMaxLength)
                return BreakingChanges.STRING_TYPE_MAX_LENGTH_VALUE_DECREASED;
        }
        return null;
    }
    
    private BreakingChanges patternComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("pattern") != null && toCheckAgainst.get("pattern") == null)
            return BreakingChanges.STRING_TYPE_PATTERN_ADDED;
        else if (toCheck.get("pattern") != null && toCheckAgainst.get("pattern") != null) {
            if(!toCheck.get("pattern").asText().equals(toCheckAgainst.get("pattern").asText()))
                return BreakingChanges.STRING_TYPE_PATTERN_MODIFIED;
        }
        return null;
    }
}
