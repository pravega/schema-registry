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
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionType;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * GetEncodingIdRequest
 */

public class GetEncodingIdRequest   {
  @JsonProperty("subgroupName")
  private String subgroupName = null;

  @JsonProperty("versionInfo")
  private VersionInfo versionInfo = null;

  @JsonProperty("compressionType")
  private CompressionType compressionType = null;

  public GetEncodingIdRequest subgroupName(String subgroupName) {
    this.subgroupName = subgroupName;
    return this;
  }

  /**
   * Get subgroupName
   * @return subgroupName
   **/
  @JsonProperty("subgroupName")
  @ApiModelProperty(value = "")
  public String getSubgroupName() {
    return subgroupName;
  }

  public void setSubgroupName(String subgroupName) {
    this.subgroupName = subgroupName;
  }

  public GetEncodingIdRequest versionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
    return this;
  }

  /**
   * Get versionInfo
   * @return versionInfo
   **/
  @JsonProperty("versionInfo")
  @ApiModelProperty(value = "")
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  public void setVersionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
  }

  public GetEncodingIdRequest compressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * Get compressionType
   * @return compressionType
   **/
  @JsonProperty("compressionType")
  @ApiModelProperty(value = "")
  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetEncodingIdRequest getEncodingIdRequest = (GetEncodingIdRequest) o;
    return Objects.equals(this.subgroupName, getEncodingIdRequest.subgroupName) &&
        Objects.equals(this.versionInfo, getEncodingIdRequest.versionInfo) &&
        Objects.equals(this.compressionType, getEncodingIdRequest.compressionType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subgroupName, versionInfo, compressionType);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetEncodingIdRequest {\n");
    
    sb.append("    subgroupName: ").append(toIndentedString(subgroupName)).append("\n");
    sb.append("    versionInfo: ").append(toIndentedString(versionInfo)).append("\n");
    sb.append("    compressionType: ").append(toIndentedString(compressionType)).append("\n");
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

