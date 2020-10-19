package io.pravega.schemaregistry.rules.jsoncompatibility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BreakingChangesStore {
    //include only those changes that lead to incompatibility
    protected enum BreakingChanges {
        // strings
        MAX_LENGTH_ADDED,
        MAX_LENGTH_DECREASED,
        MIN_LENGTH_ADDED,
        MIN_LENGTH_INCREASED,
        PATTERN_ADDED,
        PATTERN_CHANGED,
        // NUMBERS
        MAXIMUM_ADDED,
        MAXIMUM_DECREASED,
        EXCLUSIVE_MAXIMUM_ADDED,
        EXCLUSIVE_MAXIMUM_DECREASED,
        MINIMUM_ADDED,
        MINIMUM_INCREASED,
        EXCLUSIVE_MINIMUM_ADDED,
        EXCLUSIVE_MINIMUM_INCREASED,
        MULTIPLE_OF_ADDED,
        MULTIPLE_OF_CHANGED,
        MULTIPLE_OF_EXPANDED,
        TYPE_NARROWED,
        //ARRAYS
        MAX_ITEMS_ADDED,
        MAX_ITEMS_DECREASED,
        MIN_ITEMS_ADDED,
        MIN_ITEMS_INCREASED,
        UNIQUE_ITEMS_ADDED,
        ADDITIONAL_ITEMS_REMOVED,
        ADDITIONAL_ITEMS_NARROWED,
        ITEMS_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL,
        ITEM_ADDED_TO_OPEN_CONTENT_MODEL,
        ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        // PROPERTIES
        PROPERTIES_SECTION_ADDED,     // this would be taken care of by checking for required and additional properties since that is when incompatibility arises
        PROPERTY_REMOVED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION, // check on updated schema
        PROPERTY_REMOVED_FROM_STATIC_PROPERTY_SET,   // check on updated schema
        PROPERTY_ADDED_TO_DYNAMIC_PROPERTY_SET, // check on original schema
        PROPERTY_ADDED_NOT_PART_OF_DYNAMIC_PROPERTY_SET_WITH_CONDITION,   // check on original schema
        REQUIRED_PROPERTY_ADDED_WITHOUT_DEFAULT,
        REQUIRED_ATTRIBUTE_ADDED, // may not be needed
        MAX_PROPERTIES_ADDED,
        MAX_PROPERTIES_LIMIT_DECREASED,
        MIN_PROPERTIES_ADDED,
        MIN_PROPERTIES_LIMIT_INCREASED,
        ADDITIONAL_PROPERTIES_REMOVED,
        ADDITIONAL_PROPERTIES_NARROWED,
        // DEPENDENCIES
        DEPENDENCY_SECTION_ADDED,
        DEPENDENCY_ADDED_IN_ARRAY_FORM,
        DEPENDENCY_ARRAY_ELEMENTS_CHANGED,
        DEPENDENCY_ADDED_IN_SCHEMA_FORM,
        DEPENDENCY_IN_SCHEMA_FORM_MODIFIED,
        // ENUM
        ENUM_ARRAY_NARROWED,
        ENUM_ARRAY_CHANGED,
        // NOT TYPE
        NOT_TYPE_EXTENDED,
        // COMBINED
        COMBINED_TYPE_SUBSCHEMAS_CHANGED,
        COMPOSITION_METHOD_CHANGED,
        PRODUCT_TYPE_EXTENDED,
        SUM_TYPE_NARROWED
    }
    
    private List<BreakingChanges> breakingChangesList = new ArrayList<>();
    
    private void computeBreakingChanges() {
        breakingChangesList = Arrays.asList(BreakingChanges.values());
    }
    
    public List<BreakingChanges> getBreakingChangesList() {
        return breakingChangesList;
    }
}
