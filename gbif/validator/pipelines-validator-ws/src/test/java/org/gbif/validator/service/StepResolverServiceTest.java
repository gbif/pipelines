package org.gbif.validator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.FileFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class StepResolverServiceTest {

  private final StepResolverService stepResolverService = new StepResolverService();

  @Test
  void shouldResolveDwcdpStepsInOrder() {
    List<StepType> steps =
      List.copyOf(stepResolverService.resolve(FileFormat.DWCDP));

    assertEquals(
      List.of(
        StepType.VALIDATOR_VALIDATE_DWCDP,
        StepType.VALIDATOR_DWCDP_STAGE,
        StepType.VALIDATOR_DWCDP_TO_VERBATIM,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
        StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
        StepType.VALIDATOR_COLLECT_METRICS),
      steps);
  }

  @ParameterizedTest
  @EnumSource(
    value = FileFormat.class,
    names = {"DWCA", "TABULAR", "SPREADSHEET"})
  void shouldResolveStandardArchiveSteps(FileFormat fileFormat) {
    List<StepType> steps =
      List.copyOf(stepResolverService.resolve(fileFormat));

    assertEquals(
      List.of(
        StepType.VALIDATOR_VALIDATE_ARCHIVE,
        StepType.VALIDATOR_DWCA_TO_VERBATIM,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
        StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
        StepType.VALIDATOR_COLLECT_METRICS),
      steps);
  }

  @Test
  void shouldResolveXmlStepsInOrder() {
    List<StepType> steps =
      List.copyOf(stepResolverService.resolve(FileFormat.XML));

    assertEquals(
      List.of(
        StepType.VALIDATOR_VALIDATE_ARCHIVE,
        StepType.VALIDATOR_XML_TO_VERBATIM,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
        StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
        StepType.VALIDATOR_COLLECT_METRICS),
      steps);
  }

  @Test
  void shouldResolveColdpToArchiveValidationOnly() {
    assertEquals(
      List.of(StepType.VALIDATOR_VALIDATE_ARCHIVE),
      List.copyOf(stepResolverService.resolve(FileFormat.COLDP)));
  }

  @Test
  void shouldRejectNullFileFormat() {
    assertThrows(
      NullPointerException.class,
      () -> stepResolverService.resolve(null));
  }
}
