package au.org.ala.utils;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
public class ValidationResult {

  Boolean valid;
  String message;

  public static final ValidationResult OK =
      ValidationResult.builder().valid(true).message("OK").build();
}
