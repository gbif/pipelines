package au.org.ala.kvs.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.util.List;
import java.util.Map;

/**
 * An ALA Collectory Data Resource response object. This maps on to a data resource response e.g.
 * https://collections.ala.org.au/ws/dataResource/dr376
 */
@JsonDeserialize(builder = ALACollectoryMetadata.ALACollectoryMetadataBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ALACollectoryMetadata {

  String name;
  EntityReference provider;
  String acronym;
  String uid;
  String licenseType;
  String licenseVersion;
  String provenance;

  ConnectionParameters connectionParameters;
  Map<String, String> defaultDarwinCoreValues;
  List<Map<String, String>> taxonomyCoverageHints;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ALACollectoryMetadataBuilder {

  }

  public static final ALACollectoryMetadata EMPTY = ALACollectoryMetadata.builder().build();
}
