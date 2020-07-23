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

set -eo pipefail

DIR=/opt/schema-registry
SCRIPTS_DIR=${DIR}/scripts

source ${SCRIPTS_DIR}/common.sh
source ${SCRIPTS_DIR}/init_schemaregistry.sh

if [ ${WAIT_FOR} ];then
    ${SCRIPTS_DIR}/wait_for
fi

exec /opt/schema-registry/bin/schema-registry
