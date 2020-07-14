/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

@Provider
@PreMatching
/**
 * A request filter that convers namespace expressed as matrix parameter of path segment "groups"
 * into a query param. 
 * This allows users to express either "/v1/groups;namespace=ns" or "/v1/groups?namespace=ns". 
 */
public class NamespacePathRequestFilter implements ContainerRequestFilter {
	private static final String NAMESPACE = "namespace";

	@Override
	public void filter(ContainerRequestContext containerRequest)
			throws WebApplicationException {
		UriInfo uriInfo = containerRequest.getUriInfo();
		// if path has namespace defined as path segment. convert it to query param. 
		UriBuilder requestUri = uriInfo.getRequestUriBuilder();
		List<PathSegment> pathSegments = uriInfo.getPathSegments();
		if (pathSegments.size() > 1 && 
				pathSegments.get(1).getMatrixParameters().containsKey(NAMESPACE)) {
			MultivaluedMap<String, String> matrixParams = pathSegments.get(1).getMatrixParameters();
			String namespace = matrixParams.getFirst(NAMESPACE);
			StringBuilder pathBuilder = new StringBuilder();
			for (PathSegment pathSegment : pathSegments) {
				pathBuilder.append("/");
	
				pathBuilder.append(pathSegment.getPath());
				pathSegment.getMatrixParameters().forEach((x, y) -> {
					if (!x.equals(NAMESPACE)) {
						pathBuilder.append(";").append(x).append("=").append(y);
					}
				});
			}
			requestUri.replacePath(pathBuilder.toString());
			requestUri.queryParam(NAMESPACE, namespace);
		}
		containerRequest.setRequestUri(requestUri.build());	
	}
}