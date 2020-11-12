package au.org.ala.kvs.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/** DTO for ALA collectory connection parameters. */
@JsonDeserialize(builder = ConnectionParameters.ConnectionParametersBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionParameters {

  String protocol;

  List<String> url;

  List<String> termsForUniqueKey;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectionParametersBuilder {}
}
