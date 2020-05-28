/*
 * Pravega Schema Registry APIs
 * REST APIs for Pravega Schema Registry.
 *
 * OpenAPI spec version: 0.0.1
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.pravega.schemaregistry.contract.generated.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * Group id with schema version.
 */
@ApiModel(description = "Group id with schema version.")

public class SchemaReferenceRecord   {
  @JsonProperty("groupId")
  private String groupId = null;

  @JsonProperty("version")
  private VersionInfo version = null;

  public SchemaReferenceRecord groupId(String groupId) {
    this.groupId = groupId;
    return this;
  }

  /**
   * Group id.
   * @return groupId
   **/
  @JsonProperty("groupId")
  @ApiModelProperty(required = true, value = "Group id.")
  @NotNull
  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public SchemaReferenceRecord version(VersionInfo version) {
    this.version = version;
    return this;
  }

  /**
   * Schema version information object.
   * @return version
   **/
  @JsonProperty("version")
  @ApiModelProperty(required = true, value = "Schema version information object.")
  @NotNull
  public VersionInfo getVersion() {
    return version;
  }

  public void setVersion(VersionInfo version) {
    this.version = version;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaReferenceRecord schemaReferenceRecord = (SchemaReferenceRecord) o;
    return Objects.equals(this.groupId, schemaReferenceRecord.groupId) &&
        Objects.equals(this.version, schemaReferenceRecord.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, version);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaReferenceRecord {\n");
    
    sb.append("    groupId: ").append(toIndentedString(groupId)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

