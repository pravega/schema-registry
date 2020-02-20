/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import io.pravega.shared.NameUtils;

public class TableNames {
    public static final String NAMESPACES_TABLE = NameUtils.INTERNAL_SCOPE_NAME + "/" + "_namespaces/0";
    public static final String NAMESPACE_TABLE_FORMAT = NameUtils.INTERNAL_SCOPE_NAME + "/" + "_namespace_%s/0";
    public static final String GROUP_TABLE_FORMAT = NameUtils.INTERNAL_SCOPE_NAME + "/" + "_namespace_%s_group_%s/0";
}
