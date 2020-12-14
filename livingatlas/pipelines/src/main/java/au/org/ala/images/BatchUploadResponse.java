package au.org.ala.images;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/** An ALA Collection Match response object. */
@JsonDeserialize(builder = BatchUploadResponse.BatchUploadResponseBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchUploadResponse {

  String batchID;
  String dataResourceUid;
  String status;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class BatchUploadResponseBuilder {}
}
