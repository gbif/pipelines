package org.gbif.pipelines.tasks.occurrences.identifier.validation;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(staticName = "create")
public class IdentifierValidationResult {

  private final double totalRecords;
  private final double absentIdentifierRecords;
  private final boolean isResultValid;
  private final String validationMessage;
}
