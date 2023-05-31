package au.org.ala.kvs.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

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
  String doi;
  String resourceType;
  String licenseType;
  String licenseVersion;
  String provenance;
  String dateCreated;

  ConnectionParameters connectionParameters;
  Map<String, String> defaultDarwinCoreValues;
  List<Map<String, String>> taxonomyCoverageHints;
  List<EntityReference> hubMembership;
  List<String> contentTypes;

  /**
   * Build a map of hints suitable for a name search.
   *
   * @return a map of hints.
   */
  @JsonIgnore
  public Map<String, List<String>> getHintMap() {
    if (this.taxonomyCoverageHints == null || this.taxonomyCoverageHints.isEmpty()) {
      return Collections.emptyMap();
    }

    return this.taxonomyCoverageHints.stream()
        .flatMap(element -> element.entrySet().stream())
        .collect(
            Collectors.groupingBy(
                entry -> entry.getKey().toLowerCase(),
                Collectors.mapping(
                    entry -> entry.getValue().trim().toLowerCase(), Collectors.toList())));
  }

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ALACollectoryMetadataBuilder {}

  public static final ALACollectoryMetadata EMPTY = ALACollectoryMetadata.builder().build();
}
