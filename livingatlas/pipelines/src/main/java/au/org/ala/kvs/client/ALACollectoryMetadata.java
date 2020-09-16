package au.org.ala.kvs.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.*;
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
  String licenseType;
  String licenseVersion;
  String provenance;

  ConnectionParameters connectionParameters;
  Map<String, String> defaultDarwinCoreValues;
  List<Map<String, String>> taxonomyCoverageHints;

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

    Map<String, List<String>> hints = new HashMap<>();
    for (Map<String, String> element : this.taxonomyCoverageHints) {
      for (Map.Entry<String, String> entry : element.entrySet()) {
        hints
            .computeIfAbsent(entry.getKey().toLowerCase(), k -> new ArrayList<>())
            .add(entry.getValue().trim().toLowerCase());
      }
    }
    return hints;
  }

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ALACollectoryMetadataBuilder {}

  public static final ALACollectoryMetadata EMPTY = ALACollectoryMetadata.builder().build();
}
