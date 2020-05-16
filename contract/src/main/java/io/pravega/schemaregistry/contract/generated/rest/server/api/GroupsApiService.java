package io.pravega.schemaregistry.contract.generated.rest.server.api;

import io.pravega.schemaregistry.contract.generated.rest.server.api.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaNamesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;

import java.util.List;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class GroupsApiService {
    public abstract Response addCodec(String groupName,AddCodec addCodec,SecurityContext securityContext) throws NotFoundException;
    public abstract Response addSchema(String groupName,AddSchemaRequest addSchemaRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response canRead(String groupName,CanReadRequest canReadRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createGroup(CreateGroupRequest createGroupRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteGroup(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getCodecsList(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingId(String groupName,GetEncodingIdRequest getEncodingIdRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getEncodingInfo(String groupName,Integer encodingId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupHistory(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getGroupProperties(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestSchema(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLatestSchemaForSchemaName(String groupName,String schemaName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaFromVersion(String groupName,Integer version,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaNames(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaValidationRules(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemaVersion(String groupName,GetSchemaVersion getSchemaVersion,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemas(String groupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getSchemasForSchemaName(String groupName,String schemaName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listGroups( String continuationToken, Integer limit,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateSchemaValidationRules(String groupName,UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response validate(String groupName,ValidateRequest validateRequest,SecurityContext securityContext) throws NotFoundException;
}
