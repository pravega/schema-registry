package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.rules.CompatibilityChecker;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.*;

public class JsonCompatibilityChecker implements CompatibilityChecker {
    private static List<String> characteristics = Arrays.asList("properties", "dependencies", "required",
            "additionalProperties");
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();

    @Override
    public boolean canRead(SchemaInfo readUsing, List<SchemaInfo> writtenUsing) {
        return false;
    }

    @Override
    public boolean canBeRead(SchemaInfo writtenUsing, List<SchemaInfo> readUsing) {
        return false;
    }

    @Override
    public boolean canMutuallyRead(SchemaInfo schema, List<SchemaInfo> schemaList) {
        return false;
    }

    private enum PossibleDifferences {
        VARIABLE_TYPE_CHANGED,
        VARIABLE_ADDED,
        VARIABLE_DELETED,
        DEPENDENCIES_ADDED,
        DEPENDENCIES_REMOVED,
        DEPENDENCY_VALUE_ADDED,
        DEPENDENCY_VALUE_REMOVED,
        DEPENDENCY_KEY_ADDED,
        DEPENDENCY_KEY_REMOVED,
        ARRAY_ELEMENT_ADDED,
        ARRAY_ELEMENT_REMOVED,
        ARRAY_ELEMENT_CHANGED,
        PROPERTIES_ADDED,
        PROPERTIES_REMOVED,
        PROPERTY_ADDED,
        PROPERTY_REMOVED,
        PROPERTY_CHANGED,
        REQUIREMENTS_ADDED,
        REQUIREMENTS_REMOVED,
        REQUIRED_VALUE_ADDED,
        REQUIRED_VALUE_REMOVED
    }

    Queue<String> path = new ArrayDeque<>();

    public void relay(SchemaInfo toValidate, SchemaInfo toValidateAgainst) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode toCheck = objectMapper.readTree(toValidate.getSchemaData().array());
        JsonNode toCheckAgainst = objectMapper.readTree(toValidateAgainst.getSchemaData().array());
        ArrayList<PossibleDifferences> ans = getDifferences(toCheck, toCheckAgainst);
        if (!ans.isEmpty()) {
            System.out.println("Invalid");
            //temp print statement
            System.out.println(ans);
        } else {
            System.out.println("Valid");
        }
        // toCheckAgainst

        // compute compatibility

    }

    protected BreakingChanges checkNodeType(JsonNode toCheck, JsonNode toCheckAgainst) {
        String nodeType = jsonCompatibilityCheckerUtils.getTypeValue(toCheck).equals(
                jsonCompatibilityCheckerUtils.getTypeValue(
                        toCheckAgainst)) ? jsonCompatibilityCheckerUtils.getTypeValue(toCheck) : "mismatch";
        switch (nodeType) {
            case "object":
                break;
            case "number":
            case "integer":
                break;
            case "string":
                break;
            case "array":
                break;
            case "boolean":
                break;
            case "null":
                break;
            case "mismatch":
                break;
        }
        return null;
    }

    private ArrayList<PossibleDifferences> getDifferences(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        //cover cases when either are null
        if (checkOrdinaryObject(toCheck, toCheckAgainst)) {
            differences.addAll(getOrdinaryObjectProperties(toCheck, toCheckAgainst));
            return differences;
        }
        //check for properties
        ArrayList<PossibleDifferences> propDifferences = compareProperties(toCheck, toCheckAgainst);
        if (!propDifferences.isEmpty())
            differences.addAll(propDifferences);
        //check for required
        ArrayList<PossibleDifferences> reqDifferences = compareRequired(toCheck, toCheckAgainst);
        if (!reqDifferences.isEmpty())
            differences.addAll(reqDifferences);
        //check for dependencies
        ArrayList<PossibleDifferences> dependencyDifferences = compareDependencies(toCheck, toCheckAgainst);
        if (!dependencyDifferences.isEmpty())
            differences.addAll(dependencyDifferences);
        //when both are values
        if (toCheck.isValueNode() && toCheckAgainst.isValueNode()) {
            if (!toCheck.asText().equals(toCheckAgainst.asText())) {
                differences.add(PossibleDifferences.VARIABLE_TYPE_CHANGED);
            }
        }
        //when both are arrays
        if (toCheck.isArray() && toCheckAgainst.isArray())
            differences.addAll(getArrayDifferences(toCheck, toCheckAgainst));
        return differences;
    }

    private ArrayList<PossibleDifferences> getArrayDifferences(JsonNode toCheck, JsonNode toCheckAgainst) {
        HashSet<JsonNode> toCheckCollection = new HashSet<>();
        HashSet<JsonNode> toCheckAgainstCollection = new HashSet<>();
        ArrayNode arrayNodeToCheck = (ArrayNode) toCheck;
        ArrayNode arrayNodeToCheckAgainst = (ArrayNode) toCheckAgainst;
        for (int i = 0; i < arrayNodeToCheck.size(); i++) {
            toCheckCollection.add(arrayNodeToCheck.get(i));
        }
        for (int i = 0; i < arrayNodeToCheckAgainst.size(); i++) {
            toCheckAgainstCollection.add(arrayNodeToCheckAgainst.get(i));
        }
        ArrayList<PossibleDifferences> differences = new ArrayList<>(
                Sets.difference(toCheckCollection, toCheckAgainstCollection).stream().map(
                        node -> PossibleDifferences.ARRAY_ELEMENT_ADDED).collect(
                        Collectors.toCollection(ArrayList::new)));
        Sets.difference(toCheckAgainstCollection, toCheckCollection).stream().map(
                node -> PossibleDifferences.ARRAY_ELEMENT_REMOVED).forEach(differences::add);
        return differences;
    }

    private ArrayList<PossibleDifferences> compareProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        toCheck = toCheck.get("properties");
        toCheckAgainst = toCheckAgainst.get("properties");
        if (toCheck == null && toCheckAgainst != null) {
            differences.add(PossibleDifferences.PROPERTIES_REMOVED);
            return differences;
        } else if (toCheck != null && toCheckAgainst == null) {
            differences.add(PossibleDifferences.PROPERTIES_ADDED);
            return differences;
        } else if (toCheck == null && toCheckAgainst == null) return differences;
        Iterator<String> keys = Iterators.concat(toCheck.fieldNames(), toCheckAgainst.fieldNames());
        while (keys.hasNext()) {
            String key = keys.next();
            //System.out.println(key);
            if (toCheck.get(key) == null && toCheckAgainst.get(key) != null)
                differences.add(PossibleDifferences.PROPERTY_REMOVED);
            else if (toCheck.get(key) != null && toCheckAgainst.get(key) == null)
                differences.add(PossibleDifferences.PROPERTY_ADDED);
            else {
                if (!toCheck.get(key).equals(toCheckAgainst.get(key))) {
                    differences.addAll(getDifferences(toCheck.get(key), toCheckAgainst.get(key)));
                    differences.add(PossibleDifferences.PROPERTY_CHANGED);
                }
            }
        }
        return differences;
    }

    private ArrayList<PossibleDifferences> compareRequired(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        toCheck = toCheck.get("required");
        toCheckAgainst = toCheckAgainst.get("required");
        if (toCheck == null && toCheckAgainst == null) return differences;
        else if (toCheck != null && toCheckAgainst == null) {
            differences.add(PossibleDifferences.REQUIREMENTS_ADDED);
            return differences;
        } else if (toCheck == null && toCheckAgainst != null) {
            differences.add(PossibleDifferences.REQUIREMENTS_REMOVED);
            return differences;
        }
        ArrayList<PossibleDifferences> differencesReq = getArrayDifferences(toCheck, toCheckAgainst);
        for (PossibleDifferences possibleDifference : differencesReq) {
            switch (possibleDifference) {
                case ARRAY_ELEMENT_ADDED:
                    differences.add(PossibleDifferences.REQUIRED_VALUE_ADDED);
                    break;
                case ARRAY_ELEMENT_REMOVED:
                    differences.add(PossibleDifferences.REQUIRED_VALUE_REMOVED);
                    break;
            }
        }
        return differences;
    }

    private ArrayList<PossibleDifferences> compareDependencies(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        toCheck = toCheck.get("dependencies");
        toCheckAgainst = toCheckAgainst.get("dependencies");
        if (toCheck == null && toCheckAgainst == null)
            return differences;
        else if (toCheck == null && toCheckAgainst != null) {
            differences.add(PossibleDifferences.DEPENDENCIES_REMOVED);
            return differences;
        } else if (toCheck != null && toCheck == null) {
            differences.add(PossibleDifferences.DEPENDENCIES_ADDED);
            return differences;
        }
        String testKeyToCheck = toCheck.fieldNames().next();
        String testKeyToCheckAgainst = toCheckAgainst.fieldNames().next();
        if (toCheck.get(testKeyToCheck).isArray() && toCheckAgainst.get(testKeyToCheckAgainst).isArray())
            differences.addAll(propertyDependencies(toCheck, toCheckAgainst));
        else if (toCheck.get(testKeyToCheck).isObject() && toCheckAgainst.get(testKeyToCheckAgainst).isObject())
            differences.addAll(schemaDependencies(toCheck, toCheckAgainst));
        else
            System.err.println("dependency format mismatch");
        return differences;
    }

    private ArrayList<PossibleDifferences> propertyDependencies(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        Iterator<String> keys = Iterators.concat(toCheck.fieldNames(), toCheckAgainst.fieldNames());
        while (keys.hasNext()) {
            String key = keys.next();
            System.out.println(key);
            if (toCheck.get(key) == null && toCheckAgainst.get(key) != null)
                differences.add(PossibleDifferences.DEPENDENCY_KEY_REMOVED);
            else if (toCheck.get(key) != null && toCheckAgainst.get(key) == null)
                differences.add(PossibleDifferences.DEPENDENCY_KEY_ADDED);
            else {
                if (!toCheck.get(key).equals(toCheckAgainst.get(key))) {
                    ArrayList<PossibleDifferences> elementDifferences = getArrayDifferences(toCheck.get(key),
                            toCheckAgainst.get(key));
                    for (PossibleDifferences possibleDifference : elementDifferences) {
                        switch (possibleDifference) {
                            case ARRAY_ELEMENT_ADDED:
                                differences.add(PossibleDifferences.DEPENDENCY_VALUE_ADDED);
                                break;
                            case ARRAY_ELEMENT_REMOVED:
                                differences.add(PossibleDifferences.DEPENDENCY_VALUE_REMOVED);
                                break;
                        }
                    }
                }
            }
        }
        return differences;
    }

    private ArrayList<PossibleDifferences> schemaDependencies(JsonNode toCheck, JsonNode toCheckAgainst) {
        //treating schema dependencies and differences between them as schemas and differences between schemas 
        // respectively
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        differences.addAll(getDifferences(toCheck, toCheckAgainst));
        return differences;
    }

    private ArrayList<PossibleDifferences> getOrdinaryObjectProperties(JsonNode toCheck, JsonNode toCheckAgainst) {
        ArrayList<PossibleDifferences> differences = new ArrayList<>();
        Iterator<String> keys = Iterators.concat(toCheck.fieldNames(), toCheckAgainst.fieldNames());
        while (keys.hasNext()) {
            String key = keys.next();
            if (toCheck.get(key) == null && toCheckAgainst.get(key) != null)
                differences.add(PossibleDifferences.VARIABLE_ADDED);
            else if (toCheck.get(key) != null && toCheckAgainst.get(key) == null)
                differences.add(PossibleDifferences.VARIABLE_DELETED);
            else {
                if (!toCheck.get(key).equals(toCheckAgainst.get(key))) {
                    differences.addAll(getDifferences(toCheck.get(key), toCheckAgainst.get(key)));
                }
            }
        }
        return differences;
    }

    private boolean checkOrdinaryObject(JsonNode toCheck, JsonNode toCheckAgainst) {
        for (String field : characteristics) {
            if (toCheck.has(field) || toCheckAgainst.has(field))
                return false;
        }
        if (toCheck.isValueNode() && toCheckAgainst.isValueNode())
            return false;
        return true;
    }
}
