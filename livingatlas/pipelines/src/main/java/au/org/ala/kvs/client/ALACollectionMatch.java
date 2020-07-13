package au.org.ala.kvs.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.util.List;

/**
 * An ALA Collection Match response object.
 */
@JsonDeserialize(builder = ALACollectionMatch.ALACollectionMatchBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ALACollectionMatch {

  String collectionUid;
  String collectionName;
  String institutionUid;
  String institutionName;
  List<EntityReference> hubMembership;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ALACollectionMatchBuilder {

  }

  public static final ALACollectionMatch EMPTY = ALACollectionMatch.builder().build();
}
