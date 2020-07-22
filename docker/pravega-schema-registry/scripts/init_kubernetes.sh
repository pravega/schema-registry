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

k8() {
    local namespace=$1
    local resource_type=$2
    local resource_name=$3
    local jsonpath=$4
    local bearer=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    local cacert="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    if [ -z "$namespace" ]; then
        retval=$( curl --cacert ${cacert} -H "Authorization: Bearer ${bearer}" https://kubernetes.default.svc/api/v1/${resource_type}/${resource_name} 2> /dev/null | jq -rM "${jsonpath}" 2> /dev/null )
    else
        retval=$( curl --cacert ${cacert} -H "Authorization: Bearer ${bearer}" https://kubernetes.default.svc/api/v1/namespaces/${namespace}/${resource_type}/${resource_name} 2> /dev/null | jq -rM "${jsonpath}" 2> /dev/null )
    fi

    if [ "$retval" == "null" ]; then
        retval=""
    fi

    echo "$retval"
}
