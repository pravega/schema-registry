package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateNamespaceRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromSubgroupVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespacesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.SubgroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class NamespacesApiService {
    public abstract Response addSchemaToGroupIfAbsent(String namespaceName,String groupName,AddSchemaToGroupRequest addSchemaToGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createGroup(String namespaceName,CreateGroupRequest createGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createNamespace(CreateNamespaceRequest createNamespaceRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteGroup(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteNamespace(String namespaceName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getCompressionsList(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingInfo(String namespaceName,String groupName,Integer encodingId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupProperties(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupSchemas(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestGroupSchema(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestSubgroupSchema(String namespaceName,String groupName,String subgroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getOrGenerateEncodingId(String namespaceName,String groupName,GetEncodingIdRequest getEncodingIdRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromSubgroupVersion(String namespaceName,String groupName,String subgroupName,String versionId,GetSchemaFromSubgroupVersionRequest getSchemaFromSubgroupVersionRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersion(String namespaceName,String groupName,String versionId,GetSchemaFromVersionRequest getSchemaFromVersionRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaVersion(String namespaceName,String groupName,GetSchemaVersion getSchemaVersion,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSubGroupSchemas(String namespaceName,String groupName,String subgroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSubGroups(String namespaceName,String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listGroups(String namespaceName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listNamespaces(SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateSchemaValidationRules(String namespaceName,String groupName,UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response validate(String namespaceName,String groupName,ValidateRequest validateRequest,SecurityContext securityContext) throws NotFoundException;
}
