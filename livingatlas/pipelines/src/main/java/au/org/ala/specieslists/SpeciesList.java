package au.org.ala.specieslists;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@JsonDeserialize(builder = au.org.ala.specieslists.SpeciesList.SpeciesListBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class SpeciesList {

  String dataResourceUid;
  String listName;
  String listType;
  Long itemCount;
  String region;
  String category;
  String generalisation;
  String authority;
  String sdsType;
  boolean isAuthoritative;
  boolean isInvasive;
  boolean isThreatened;
  String presentInCountry;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SpeciesListBuilder {}
}
