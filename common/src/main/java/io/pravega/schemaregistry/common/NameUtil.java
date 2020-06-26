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

public class NameUtil {
    /**
     * Extracts the name from the fully qualified type name. Name represents the last token after ".". 
     * If the qualified name does not contain "." then the name is same as qualified name. 
     * 
     * @param qualifiedName qualified name to extract name from. 
     * @return extracted name. 
     */
    public static String extractName(String qualifiedName) {
        int nameStart = qualifiedName.lastIndexOf(".");
        return qualifiedName.substring(nameStart + 1);
    }

    /**
     * Extracts name and the prefix qualifier before the name. Name represents the last token after ".".
     * Qualifier is the prefix before the name. 
     * If the qualified name does not contain "." then the name is same as qualified name and qualifier is empty string. 
     * 
     * @param qualifiedName qualified name to extract tokens from. 
     * @return an array containing name at index 0 and qualifier at index 1. 
     */
    public static String[] extractNameAndQualifier(String qualifiedName) {
        int nameStart = qualifiedName.lastIndexOf(".");
        String name = qualifiedName.substring(nameStart + 1);
        String pckg = nameStart < 0 ? "" : qualifiedName.substring(0, nameStart);
        return new String[]{name, pckg};
    }
}
