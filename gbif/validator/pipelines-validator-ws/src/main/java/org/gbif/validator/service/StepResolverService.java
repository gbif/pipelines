package org.gbif.validator.service;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.FileFormat;
import org.springframework.stereotype.Service;

@Service
public class StepResolverService {

  public Set<StepType> resolve(FileFormat fileFormat) {
    Objects.requireNonNull(fileFormat, "fileFormat must not be null");

    return switch (fileFormat) {
      case COLDP -> Set.of(StepType.VALIDATOR_VALIDATE_ARCHIVE);

      case DWCDP -> new LinkedHashSet<>(
          List.of(
              StepType.VALIDATOR_VALIDATE_DWCDP,
              StepType.VALIDATOR_DWCDP_STAGE,
              StepType.VALIDATOR_DWCDP_TO_VERBATIM,
              StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
              StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
              StepType.VALIDATOR_COLLECT_METRICS));

      case DWCA, TABULAR, SPREADSHEET -> standardPipeline(StepType.VALIDATOR_DWCA_TO_VERBATIM);

      case XML -> standardPipeline(StepType.VALIDATOR_XML_TO_VERBATIM);
    };
  }

  private Set<StepType> standardPipeline(StepType conversionStep) {
    return new LinkedHashSet<>(
        List.of(
            StepType.VALIDATOR_VALIDATE_ARCHIVE,
            conversionStep,
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
            StepType.VALIDATOR_COLLECT_METRICS));
  }
}
