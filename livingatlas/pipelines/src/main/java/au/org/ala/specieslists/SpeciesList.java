package au.org.ala.specieslists;

import com.fasterxml.jackson.annotation.JsonAlias;
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
  // id is the new dataResourceUid. New species-lists is returning both so an alias will not work here.
  String id;

  String dataResourceUid;
  @JsonAlias("title")
  String listName;
  String listType;
  @JsonAlias("rowCount")
  Long itemCount;
  String region;
  String category;
  String generalisation;
  String authority;
  String sdsType;
  boolean isAuthoritative;
  boolean isInvasive;
  boolean isThreatened;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SpeciesListBuilder {}
}
