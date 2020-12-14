package au.org.ala.specieslists;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@JsonDeserialize(
    builder = au.org.ala.specieslists.ListSearchResponse.ListSearchResponseBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListSearchResponse {

  List<SpeciesList> lists;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ListSearchResponseBuilder {}
}
