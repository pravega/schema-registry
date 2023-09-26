package io.pravega.schemaregistry.rules.jsoncompatibility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.rules.CompatibilityChecker;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.rules.jsoncompatibility.BreakingChangesStore.BreakingChanges;

public class JsonCompatibilityChecker implements CompatibilityChecker {
    EnumComparator enumComparator;
    JsonCompatibilityCheckerUtils jsonCompatibilityCheckerUtils;
    ObjectTypeComparator objectTypeComparator;
    NumberComparator numberComparator;
    StringComparator stringComparator;
    ArrayTypeComparator arrayTypeComparator;
    SubSchemaComparator subSchemaComparator;
    
    public JsonCompatibilityChecker() {
        this.enumComparator = new EnumComparator();
        this.jsonCompatibilityCheckerUtils = new JsonCompatibilityCheckerUtils();
        this.objectTypeComparator = new ObjectTypeComparator();
        this.numberComparator = new NumberComparator();
        this.stringComparator = new StringComparator();
        this.arrayTypeComparator = new ArrayTypeComparator();
        this.subSchemaComparator = new SubSchemaComparator();
    }

    @Override
    public boolean canRead(SchemaInfo readUsing, List<SchemaInfo> writtenUsing) {
        try {
            return canReadChecker(readUsing, writtenUsing);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean canBeRead(SchemaInfo writtenUsing, List<SchemaInfo> readUsing) {
        return readUsing.stream().map(x -> canRead(x, Collections.singletonList(writtenUsing))).allMatch(x -> x.equals(true));
    }

    @Override
    public boolean canMutuallyRead(SchemaInfo schema, List<SchemaInfo> schemaList) {
        return schemaList.stream().map(x -> canRead(schema, Collections.singletonList(x)) && canBeRead(x,
                Collections.singletonList(schema))).allMatch(x -> x.equals(true));
    }


    private boolean canReadChecker(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode toCheck = objectMapper.readTree(toValidate.getSchemaData().array());
        List<JsonNode> toCheckAgainst = toValidateAgainst.stream().map(x -> {
            try {
                return objectMapper.readTree(x.getSchemaData().array());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(
                Collectors.toList());
        return !toCheckAgainst.stream().map(x -> checkNodeType(toCheck, x)).anyMatch(x -> x != null);
    }

    protected BreakingChanges checkNodeType(JsonNode toCheck, JsonNode toCheckAgainst) {
        if(jsonCompatibilityCheckerUtils.hasSubSchema(toCheck)) {
            subSchemaComparator.setJsonCompatibilityChecker();
            BreakingChanges subSchemaChanges = subSchemaComparator.checkSubSchemas(toCheck, toCheckAgainst);
            if(subSchemaChanges != null)
                return subSchemaChanges;
        }
        if (toCheck.has("enum") || toCheckAgainst.has("enum")) {
            BreakingChanges enumChanges = enumComparator.enumComparator(toCheck, toCheckAgainst);
            if (enumChanges != null)
                return enumChanges;
        }
        String nodeType = jsonCompatibilityCheckerUtils.getTypeValue(toCheck).equals(
                jsonCompatibilityCheckerUtils.getTypeValue(
                        toCheckAgainst)) ? jsonCompatibilityCheckerUtils.getTypeValue(toCheck) : "mismatch";
        switch (nodeType) {
            case "object":
                return objectTypeComparator.checkAspects(toCheck, toCheckAgainst);
            case "number":
            case "integer":
                return numberComparator.compareNumbers(toCheck, toCheckAgainst);
            case "string":
                return stringComparator.stringComparator(toCheck, toCheckAgainst);
            case "array":
                arrayTypeComparator.setJsonTypeComparator();
                return arrayTypeComparator.compareArrays(toCheck, toCheckAgainst);
            case "boolean":
                break;
            case "null":
                break;
            case "mismatch":
                return analyzeMismatch(toCheck, toCheckAgainst);
        }
        return null;
    }

    private BreakingChanges analyzeMismatch(JsonNode toCheck, JsonNode toCheckAgainst) {
        if ((jsonCompatibilityCheckerUtils.getTypeValue(toCheck).equals(
                "number") || jsonCompatibilityCheckerUtils.getTypeValue(toCheck).equals(
                "integer")) && jsonCompatibilityCheckerUtils.getTypeValue(toCheckAgainst).equals(
                "number") || jsonCompatibilityCheckerUtils.getTypeValue(toCheckAgainst).equals("integer"))
            return numberComparator.compareNumbers(toCheck, toCheckAgainst);
        else
            return BreakingChanges.DATA_TYPE_MISMATCH;
    }

}
