package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterators;

import java.util.Iterator;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class DependenciesComparator {
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();
    PropertiesComparator propertiesComparator = new PropertiesComparator();
    public BreakingChanges checkDependencies(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(toCheck.get("dependencies") != null && toCheckAgainst.get("dependencies") == null)
            return BreakingChanges.DEPENDENCY_SECTION_ADDED;
        else if(toCheck.get("dependencies") == null && toCheckAgainst.get("dependencies") == null)
            return null;
        else if(toCheck.get("dependencies") == null && toCheckAgainst.get("dependencies") != null)
            return null;
        Iterator<String> dependencyFields = Iterators.concat(toCheck.get("dependencies").fieldNames(), toCheckAgainst.get("dependencies").fieldNames());
        boolean dependencyTypeIsArray = checkDependencyTypeIsArray(toCheck.get("dependencies"), toCheckAgainst.get("dependencies"));
        while(dependencyFields.hasNext()) {
            String field = dependencyFields.next();
               if(toCheck.get("dependencies").get(field) != null && toCheckAgainst.get("dependencies").get(field)==null) {
                   if(dependencyTypeIsArray)
                       return BreakingChanges.DEPENDENCY_ADDED_IN_ARRAY_FORM;
                   else 
                       return BreakingChanges.DEPENDENCY_ADDED_IN_SCHEMA_FORM;
               }
               else if (toCheck.get("dependencies").get(field) != null && toCheckAgainst.get("dependencies").get(field) != null) {
                   if(dependencyTypeIsArray) {
                       // check the value returned by the array comparator
                       if(!jsonCompatibilityCheckerUtils.arrayComparisionOnlyRemoval((ArrayNode) toCheck.get("dependencies").get(field),
                               (ArrayNode) toCheckAgainst.get("dependencies").get(field)))
                           return BreakingChanges.DEPENDENCY_ARRAY_ELEMENTS_NON_REMOVAL;
                   }
                   else {
                       if(propertiesComparator.checkProperties(toCheck.get("dependencies").get(field),
                               toCheckAgainst.get("dependencies").get(field)) != null)
                           return BreakingChanges.DEPENDENCY_IN_SCHEMA_FORM_MODIFIED;
                   }
               }
        }
        return null;
    }
    
    private boolean checkDependencyTypeIsArray(JsonNode toCheck, JsonNode toCheckAgainst) {
        // it is assumed that the dependency type does not change from array to schema or vice versa.
        Iterator<String> toCheckFields = toCheck.fieldNames();
        String toCheckSample = toCheckFields.next();
        Iterator<String> toCheckAgainstFields = toCheckAgainst.fieldNames();
        String toCheckAgainstSample = toCheckAgainstFields.next();
        if(toCheck.get(toCheckSample).isArray() && toCheckAgainst.get(toCheckAgainstSample).isArray())
            return true;
        return false;
    }
}
