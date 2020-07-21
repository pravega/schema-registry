#!/bin/sh
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

init_schemaregistry() {
    add_system_property "schema-registry.controller.server.url" "${CONTROLLER_URL}"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
