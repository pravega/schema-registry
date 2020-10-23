package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class ObjectTypeComparator {
    
    public BreakingChanges checkAspects(JsonNode toCheck, JsonNode toCheckAgainst) {
        // will check for properties,dependencies, required, additional properties by calling required classes.
        PropertiesComparator propertiesComparator = new PropertiesComparator();
        DependenciesComparator dependenciesComparator = new DependenciesComparator();
        BreakingChanges propertiesDifference = propertiesComparator.checkProperties(toCheck, toCheckAgainst);    
        if (propertiesDifference != null)
            return propertiesDifference;
        BreakingChanges dependenciesDifference = dependenciesComparator.checkDependencies(toCheck, toCheckAgainst);
        if(dependenciesDifference != null)
            return dependenciesDifference;
        return null;
    }
    
}
