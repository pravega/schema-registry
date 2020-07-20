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
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import java.util.List;

/**
 * A request filter that converts namespace expressed as a path param into a query param. 
 * 
 * This allows users to express either "/v1/namespace/ns/groups" "/v1/groups?namespace=ns" and both are equivalent. 
 * 
 * If both path param and query param are used to supply values for namespace, then the path param overrides the query param. 
 * So if someone expresses the uri as "/v1/namespace/ns1/groups?namespace=ns2". This will result in namespace being 
 * set to ns1 for the request.  
 */
@Provider
@PreMatching
public class NamespaceRedirectFilter implements ContainerRequestFilter {
    private static final String NAMESPACE = "namespace";
    private static final String GROUPS = "groups";

    @Override
    public void filter(ContainerRequestContext containerRequest)
            throws WebApplicationException {
        UriInfo uriInfo = containerRequest.getUriInfo();
        UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
        List<PathSegment> pathSegments = uriInfo.getPathSegments();
        // if path has namespace defined as path segment. convert it to query param. 
        // namespace path param is converted to query param only when namespace name is followed by groups path segment.
        if (pathSegments.size() > 3 && pathSegments.get(1).getPath().equals(NAMESPACE) 
                && pathSegments.get(3).getPath().equals(GROUPS)) {
            handleNamespacePath(uriBuilder, pathSegments);
        }
        containerRequest.setRequestUri(uriBuilder.build());
    }

    private void handleNamespacePath(UriBuilder uriBuilder, List<PathSegment> pathSegments) {
        StringBuilder pathBuilder = new StringBuilder();
        appendPath(pathSegments.get(0), pathBuilder);
        // pathsegments(1) is namspace and pathsegments(2) is name of the namespace. 
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
}