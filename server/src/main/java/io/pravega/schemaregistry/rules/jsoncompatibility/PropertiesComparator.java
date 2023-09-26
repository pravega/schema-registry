package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterators;
import io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.BreakingChanges;

import java.util.Iterator;

public class PropertiesComparator {
    JsonCompatibilityChecker jsonCompatibilityChecker;
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();

    public void setJsonCompatibilityChecker() {
        this.jsonCompatibilityChecker = new JsonCompatibilityChecker();
    }

    public BreakingChanges checkProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        Iterator<String> propertyFields = Iterators.concat(toCheck.get("properties").fieldNames(),
                toCheckAgainst.get("properties").fieldNames());
        while (propertyFields.hasNext()) {
            String propertyField = propertyFields.next();
            if (toCheck.get("properties").get(propertyField) == null) {
                // property has been removed from toCheck
                if (jsonCompatibilityCheckerUtils.hasStaticPropertySet(toCheck))
                    return BreakingChanges.PROPERTY_REMOVED_FROM_STATIC_PROPERTY_SET;
                if (!jsonCompatibilityCheckerUtils.hasStaticPropertySet(
                        toCheck) && !jsonCompatibilityCheckerUtils.hasDynamicPropertySet(toCheck)) {
                    // assume that pattern properties always matches
                    // TODO: add regex check for pattern properties
                    BreakingChanges breakingChanges = jsonCompatibilityChecker.checkNodeType(
                            toCheck.get("additionalProperties"),
                            toCheckAgainst.get("properties").get(propertyField));
                    if (breakingChanges != null)
                        return BreakingChanges.PROPERTY_REMOVED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION;
                }
            } else if (toCheckAgainst.get("properties").get(propertyField) == null) {
                // property has been added to toCheck
                if (jsonCompatibilityCheckerUtils.hasDynamicPropertySet(toCheckAgainst))
                    return BreakingChanges.PROPERTY_ADDED_TO_DYNAMIC_PROPERTY_SET;
                //check if required property in toCheck
                if (toCheck.get("required") != null) {
                    if (jsonCompatibilityCheckerUtils.isInRequired(propertyField, toCheck)) {
                        if (toCheck.get("properties").get(propertyField).get("default") == null)
                            return BreakingChanges.REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT;
                    }
                }
                if (!jsonCompatibilityCheckerUtils.hasStaticPropertySet(
                        toCheckAgainst) && !jsonCompatibilityCheckerUtils.hasDynamicPropertySet(toCheckAgainst)) {
                    // assume that pattern properties always matches
                    // TODO: add regex check for pattern properties
                    BreakingChanges breakingChanges = jsonCompatibilityChecker.checkNodeType(
                            toCheck.get("properties").get(propertyField),
                            toCheckAgainst.get("additionalProperties"));
                    if (breakingChanges != null)
                        return BreakingChanges.PROPERTY_ADDED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION;
                }
            } else if (toCheckAgainst.get("properties").get(propertyField) != null && toCheck.get("properties").get(
                    propertyField) != null) {
                 BreakingChanges singlePropertyChanges = jsonCompatibilityChecker.checkNodeType(toCheck.get("properties").get(propertyField),
                        toCheckAgainst.get("properties").get(propertyField));
                 if(singlePropertyChanges != null)
                     return singlePropertyChanges;
            }
        }
        // check for min-max conditions on properties
        BreakingChanges minMaxBreakingChanges = minMaxProperties(toCheck, toCheckAgainst);
        if (minMaxBreakingChanges != null)
            return minMaxBreakingChanges;
        // check for additional properties
        BreakingChanges additionalPropsBreakingChanges = additionalProperties(toCheck, toCheckAgainst);
        if (additionalPropsBreakingChanges != null)
            return additionalPropsBreakingChanges;
        // check for required properties
        BreakingChanges requiredPropsBreakingChanges = requiredProperties(toCheck, toCheckAgainst);
        if (requiredPropsBreakingChanges != null)
            return requiredPropsBreakingChanges;
        return null;
    }

    private BreakingChanges minMaxProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        // minProperties
        if (toCheck.get("minProperties") != null && toCheckAgainst.get("minProperties") == null)
            return BreakingChanges.MIN_PROPERTIES_ADDED;
        else if (toCheck.get("minProperties") != null && toCheckAgainst.get("minProperties") != null) {
            if (toCheck.get("minProperties").intValue() > toCheckAgainst.get("minProperties").intValue())
                return BreakingChanges.MIN_PROPERTIES_LIMIT_INCREASED;
        }
        // maxProperties
        if (toCheck.get("maxProperties") != null && toCheckAgainst.get("maxProperties") == null)
            return BreakingChanges.MAX_PROPERTIES_ADDED;
        else if (toCheck.get("maxProperties") != null && toCheckAgainst.get("maxProperties") != null) {
            if (toCheck.get("maxProperties").intValue() < toCheckAgainst.get("maxProperties").intValue())
                return BreakingChanges.MAX_PROPERTIES_LIMIT_DECREASED;
        }
        return null;
    }

    private BreakingChanges additionalProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        if (toCheck.get("additionalProperties") == null && toCheckAgainst.get("additionalProperties") != null)
            return BreakingChanges.ADDITIONAL_PROPERTIES_REMOVED;
        else if (toCheck.get("additionalProperties") != null && toCheckAgainst.get("additionalProperties") != null) {
            BreakingChanges additionalPropertiesBreakingChanges = jsonCompatibilityChecker.checkNodeType(
                    toCheck.get("additionalProperties"), toCheckAgainst.get("additionalProperties"));
            if (additionalPropertiesBreakingChanges != null)
                return BreakingChanges.ADDITIONAL_PROPERTIES_NON_COMPATIBLE_CHANGE;
        }
        return null;
    }

    private BreakingChanges requiredProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayNode arrayNodeToCheck = (ArrayNode) toCheck.get("required");
        if (arrayNodeToCheck != null) {
            for (int i = 0; i < arrayNodeToCheck.size(); i++) {
                if (!jsonCompatibilityCheckerUtils.isInRequired(arrayNodeToCheck.get(i).textValue(), toCheckAgainst)) {
                    if (toCheck.get("properties").get(arrayNodeToCheck.get(i).textValue()).get("default") == null)
                        return BreakingChanges.REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT;
                }
            }
        }
        return null;
    }
}
