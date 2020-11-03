package io.pravega.schemaregistry.client;

import io.pravega.schemaregistry.common.AuthHelper;
import io.pravega.schemaregistry.common.CredentialProvider;
import lombok.AllArgsConstructor;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;

@AllArgsConstructor
public class AuthFilter implements ClientRequestFilter {
    private final CredentialProvider credentialProvider;
    
    @Override
    public void filter(ClientRequestContext context) {
        context.getHeaders().add(HttpHeaders.AUTHORIZATION, 
                AuthHelper.getAuthorizationHeader(credentialProvider.getMethod(), credentialProvider.getToken()));
    }
}
