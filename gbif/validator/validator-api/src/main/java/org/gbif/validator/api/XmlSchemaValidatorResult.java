package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonDeserialize(builder = XmlSchemaValidatorResult.XmlSchemaValidatorResultBuilder.class)
public class XmlSchemaValidatorResult {

  @Builder.Default private List<XmlSchemaValidatorError> errors = Collections.emptyList();

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public boolean isValid() {
    return errors.isEmpty();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  @JsonDeserialize(builder = XmlSchemaValidatorError.XmlSchemaValidatorErrorBuilder.class)
  public static class XmlSchemaValidatorError {

    public enum Level {
      WARNING,
      ERROR,
      FATAL
    }

    private Level level;

    private String error;
  }
}
