package org.gbif.validator.ws.resource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Validation;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Resource to list the vocabularies/enums used by the validation services. */
@RestController
@RequestMapping(value = "validation/enumeration", produces = MediaType.APPLICATION_JSON_VALUE)
public class EnumerationResource {

  private static final Set<String> INVENTORY =
      Stream.of(
              DwcFileType.class.getSimpleName(),
              EvaluationCategory.class.getSimpleName(),
              EvaluationType.class.getSimpleName(),
              FileFormat.class.getSimpleName(),
              DwcFileType.class.getSimpleName(),
              "ValidationStatus") // Validation.Status
          .collect(Collectors.toSet());

  @GetMapping
  public Set<String> inventory() {
    return INVENTORY;
  }

  @GetMapping(value = "EvaluationCategory")
  public EvaluationCategory[] evaluationCategories() {
    return EvaluationCategory.values();
  }

  @GetMapping(value = "EvaluationCategory/{evaluationCategory}")
  public List<EvaluationType> categoryEvaluationTypes(
      @PathVariable("evaluationCategory") EvaluationCategory evaluationCategory) {
    return listEvaluationTypes(evaluationCategory);
  }

  @GetMapping(value = "EvaluationType")
  public List<EvaluationType> evaluationTypes(
      @RequestParam(value = "evaluationCategory", required = false)
          EvaluationCategory evaluationCategory) {
    return listEvaluationTypes(evaluationCategory);
  }

  private static List<EvaluationType> listEvaluationTypes(EvaluationCategory evaluationCategory) {
    return Stream.of(EvaluationType.values())
        .filter(et -> evaluationCategory == null || evaluationCategory == et.getCategory())
        .collect(Collectors.toList());
  }

  @GetMapping(value = "DwcFileType")
  public DwcFileType[] dwcFileTypes() {
    return DwcFileType.values();
  }

  @GetMapping(value = "FileFormat")
  public FileFormat[] fileFormats() {
    return FileFormat.values();
  }

  @GetMapping(value = "ValidationStatus")
  public Validation.Status[] validationStatuses() {
    return Validation.Status.values();
  }
}
