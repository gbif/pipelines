package org.gbif.pipelines.common;

import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ValidatorPredicate {

  public static boolean isValidator(Set<String> types, boolean cliType) {
    return isValidator(types) || cliType;
  }

  public static boolean isValidator(Set<String> types) {
    return types.stream().anyMatch(t -> t.startsWith("VALIDATOR_"));
  }
}
