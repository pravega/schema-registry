/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.filter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import java.util.List;

/**
 * A request filter that convers namespace expressed as matrix parameter of path segment "groups"
 * into a query param. 
 * This allows users to express either "/v1/groups;namespace=ns" or "/v1/groups?namespace=ns". 
 * If both path segment and query param are specified, path segment takes precedence. 
 */
@Provider
@PreMatching
public class NamespacePathRequestFilter implements ContainerRequestFilter {
    private static final String NAMESPACE = "namespace";
    private static final String GROUPS = "groups";

    @Override
    public void filter(ContainerRequestContext containerRequest)
            throws WebApplicationException {
        UriInfo uriInfo = containerRequest.getUriInfo();
        // if path has namespace defined as path segment. convert it to query param. 
        UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
        List<PathSegment> pathSegments = uriInfo.getPathSegments();
        if (pathSegments.size() > 1 && pathSegments.get(1).getPath().equals(GROUPS) && 
                pathSegments.get(1).getMatrixParameters().containsKey(NAMESPACE)) {
            handlePathSegment(uriBuilder, pathSegments);
        } else if (pathSegments.size() > 2 && pathSegments.get(1).getPath().equals(NAMESPACE)) {
            handleNamespacePath(uriBuilder, pathSegments);
        }
        containerRequest.setRequestUri(uriBuilder.build());
    }

    private void handleNamespacePath(UriBuilder uriBuilder, List<PathSegment> pathSegments) {
        StringBuilder pathBuilder = new StringBuilder();
        appendPath(pathSegments.get(0), pathBuilder);

        String namespace = pathSegments.get(2).getPath();
        
        for (int i = 3; i < pathSegments.size(); i++) {
            appendPath(pathSegments.get(i), pathBuilder);
        }

        uriBuilder.replacePath(pathBuilder.toString());
        uriBuilder.replaceQueryParam(NAMESPACE, namespace);
    }

    private void appendPath(PathSegment pathSegment, StringBuilder pathBuilder) {
        pathBuilder.append("/").append(pathSegment.getPath());
        pathSegment.getMatrixParameters().forEach((x, y) -> pathBuilder.append(";").append(x).append("=").append(y));
    }

    private void handlePathSegment(UriBuilder uriBuilder, List<PathSegment> pathSegments) {
        // replace the path segment for groups (at index 1) with
        MultivaluedMap<String, String> matrixParams = pathSegments.get(1).getMatrixParameters();
        String namespace = matrixParams.getFirst(NAMESPACE);
        uriBuilder.replaceQueryParam(NAMESPACE, namespace);
    }
}