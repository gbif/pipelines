package org.gbif.dwca.validation;

import com.fasterxml.jackson.annotation.JsonFilter;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import org.xml.sax.SAXParseException;

public interface XmlSchemaValidator {

  @Data
  @Builder
  public static class ValidationError {

    public enum Level {
      WARNING,
      ERROR,
      FATAL
    }

    private final XmlSchemaValidator.ValidationError.Level level;

    @JsonFilter("stackTrace")
    private final SAXParseException error;
  }

  @Data
  @Builder
  public static class ValidationResult {

    private final List<XmlSchemaValidator.ValidationError> errors;

    public boolean isValid() {
      return errors.isEmpty();
    }
  }

  ValidationResult validate(String document);
}
