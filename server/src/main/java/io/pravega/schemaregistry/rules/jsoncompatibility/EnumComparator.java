package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class EnumComparator {
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();
    public BreakingChanges enumComparator(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.has("enum") && !toCheckAgainst.has("enum"))
            return BreakingChanges.ENUM_TYPE_ADDED;
        else if(toCheck.has("enum") && toCheck.has("enum")) {
            if(!jsonCompatibilityCheckerUtils.arrayComparisionOnlyAddition((ArrayNode)toCheck.get("enum"), (ArrayNode)toCheckAgainst.get("enum")))
                return BreakingChanges.ENUM_TYPE_ARRAY_CONTENTS_NON_ADDITION_OF_ELEMENTS;
        }
        return null;
    }
}
