/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class NameUtil {
    /**
     * Extracts the name from the fully qualified type name. Name represents the last token after ".". 
     * If the qualified name does not contain "." then the name is same as qualified name. 
     * 
     * Example: io.pravega.MyObject will return MyObject
     * Example: MyObject will return MyObject
     * 
     * @param qualifiedName qualified name to extract name from. 
     * @return extracted name. 
     */
    public static String extractName(String qualifiedName) {
        Preconditions.checkNotNull(qualifiedName, "Name cannot be null");
        return extractNameAndQualifier(qualifiedName)[0];
    }

    /**
     * Extracts name and the prefix qualifier before the name. Name represents the last token after ".".
     * Qualifier is the prefix before the name. 
     * If the qualified name does not contain "." then the name is same as qualified name and qualifier is empty string.
     * 
     * Example: io.pravega.MyObject will return ["MyObject", "io.pravega"]
     * Example: MyObject will return ["MyObject", ""]
     * 
     * @param qualifiedName qualified name to extract tokens from. 
     * @return an array containing name at index 0 and qualifier at index 1. 
     */
    public static String[] extractNameAndQualifier(String qualifiedName) {
        Preconditions.checkNotNull(qualifiedName, "Name cannot be null");
        int nameStart = qualifiedName.lastIndexOf(".");
        String name = qualifiedName.substring(nameStart + 1);
        String pckg = nameStart < 0 ? "" : qualifiedName.substring(0, nameStart);
        return new String[]{name, pckg};
    }

    /**
     * Qualified name generated as 'qualifier.name'. 
     * If qualifier is null or empty then the qualified name is simply 'name'.
     * 
     * @param qualifier optional qualifier to be used. 
     * @param name the name part
     * @return Qualified name
     */
    public static String qualifiedName(String qualifier, String name) {
        Preconditions.checkNotNull(name, "Name cannot be null");
        return Strings.isNullOrEmpty(qualifier) ? name : String.format("%s.%s", qualifier, name);
    }

    /**
     * Type value if the 'type' is not null of empty.
     * If type is null or empty then the created name is simply 'namespace.groupName'.
     *
     * @param type the value provided with API call (schemaInfo.getType()).
     * @param groupName the name of the group for schema
     * @param namespace the namespace for the schema
     * @return Provided name or Created name for type in SchemaInfo
     */
    public static String createTypeIfAbsent(String type, String groupName, String namespace) {
        return Strings.isNullOrEmpty(type) ? String.format("%s.%s", namespace, groupName) : type;
    }
}
