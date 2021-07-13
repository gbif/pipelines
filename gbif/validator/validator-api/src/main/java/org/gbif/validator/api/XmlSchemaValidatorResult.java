package org.gbif.validator.api;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class XmlSchemaValidatorResult {

  private final List<XmlSchemaValidatorError> errors;

  public boolean isValid() {
    return errors.isEmpty();
  }

  @Data
  @Builder
  public static class XmlSchemaValidatorError {

    public enum Level {
      WARNING,
      ERROR,
      FATAL
    }

    private final Level level;

    private final String error;
  }
}
