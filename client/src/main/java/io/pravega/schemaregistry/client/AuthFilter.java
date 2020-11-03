package io.pravega.schemaregistry.client;

import io.pravega.schemaregistry.common.AuthHelper;
import io.pravega.schemaregistry.common.CredentialProvider;
import lombok.AllArgsConstructor;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;

/**
 * Authentication filter for the client. This intercepts requests and adds authentication/authorization header 
 * to the request. The credentials to add are retrieved from credential provider.
 */
@AllArgsConstructor
public class AuthFilter implements ClientRequestFilter {
    private final CredentialProvider credentialProvider;
    
    @Override
    public void filter(ClientRequestContext context) {
        context.getHeaders().add(HttpHeaders.AUTHORIZATION, 
                AuthHelper.getAuthorizationHeader(credentialProvider.getMethod(), credentialProvider.getToken()));
    }
}
