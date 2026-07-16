package org.gbif.pipelines.validator.checklists;

import static org.gbif.pipelines.validator.checklists.ws.ChecklistbankWsClient.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.ws.ChecklistbankWsClient;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/** Evaluates checklists using ChecklistBank.org API. */
@Slf4j
public class ChecklistValidator {

  private static final int SAMPLE_ISSUES_SIZE = 5;

  // values to wait for validation api response
  private static final int MAX_WAIT_SECONDS = 720;
  private static final int WAIT_DELAY_IN_SECONDS = 1;
  private static final int DELAY_MULTIPLIER = 2;
  private static final int MAX_WAIT_DELAY_IN_SECONDS = 30;

  private static final String FINISHED = "finished";
  private static final String CANCELED = "canceled";
  private static final String FAILED = "failed";
  private static final List<String> FINISHED_STATES = List.of(FINISHED, CANCELED, FAILED);

  private final ChecklistbankWsClient checklistbankWsClient;

  public ChecklistValidator(String clbApiUrl, String clbApiUser, String clbApiPassword) {
    this(buildChecklistbankWsClient(clbApiUrl, clbApiUser, clbApiPassword));
  }

  public ChecklistValidator(ChecklistbankWsClient checklistbankWsClient) {
    this.checklistbankWsClient = checklistbankWsClient;
  }

  private static ChecklistbankWsClient buildChecklistbankWsClient(
      String clbApiUrl, String clbApiUser, String clbApiPassword) {
    return new ClientBuilder()
        .withUrl(clbApiUrl)
        .withCredentials(clbApiUser, clbApiPassword)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getDefaultObjectMapper())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(ChecklistbankWsClient.class);
  }

  @SneakyThrows
  public List<Metrics.FileInfo> evaluate(Path archivePath) throws IOException {
    List<Metrics.FileInfo> results = new ArrayList<>();

    ValidatorResponse validatorResponse =
        checklistbankWsClient.validateArchive(Files.readAllBytes(archivePath));
    int datasetKey = validatorResponse.getKey();

    if (datasetKey == 0) {
      throw new IllegalStateException("Validation failed with key zero for " + archivePath);
    }

    ImporterResponse importerResponse = getImporterResponse(datasetKey, archivePath);

    if (!importerResponse.getState().equalsIgnoreCase(FINISHED)) {
      throw new IllegalStateException(
          "Validation failed with status " + importerResponse.getState() + " for " + archivePath);
    }

    for (Map.Entry<Term, Map<Term, Long>> entry :
        importerResponse.getVerbatimByRowTypeCount().entrySet()) {
      Term rowType = entry.getKey();
      Map<Term, Long> terms = entry.getValue();
      List<Metrics.TermInfo> termsInfo =
          terms.entrySet().stream()
              .map(
                  e ->
                      Metrics.TermInfo.builder()
                          .term(e.getKey().qualifiedName())
                          .rawIndexed(e.getValue())
                          .build())
              .toList();

      if (rowType == DwcTerm.Taxon) {
        // core
        List<Metrics.IssueInfo> issues =
            importerResponse.getIssuesCount().entrySet().stream()
                .map(
                    e ->
                        Metrics.IssueInfo.builder()
                            .issue(e.getKey())
                            .count(e.getValue())
                            .issueCategory(EvaluationCategory.CLB_INTERPRETATION_BASED)
                            .samples(getIssueSamples(datasetKey, e.getKey()))
                            .build())
                .toList();

        results.add(
            Metrics.FileInfo.builder()
                .rowType(DwcTerm.Taxon.qualifiedName())
                .count(importerResponse.getVerbatimByTermCount().get(rowType))
                .fileName(getFileNameByRowType(datasetKey, rowType).orElse(null))
                .fileType(DwcFileType.CORE)
                .issues(issues)
                .terms(termsInfo)
                .indexedCount(importerResponse.getUsagesCount())
                .build());
      } else {
        // extensions
        results.add(
            Metrics.FileInfo.builder()
                .rowType(rowType.qualifiedName())
                .count(importerResponse.getVerbatimByTermCount().get(rowType))
                .fileName(getFileNameByRowType(datasetKey, rowType).orElse(null))
                .fileType(DwcFileType.EXTENSION)
                .terms(termsInfo)
                .indexedCount(getExtensionCount(rowType, importerResponse))
                .build());
      }
    }

    return results;
  }

  private ImporterResponse getImporterResponse(int datasetKey, Path archivePath)
      throws InterruptedException {
    ImporterResponse importerResponse = checklistbankWsClient.checkImporter(datasetKey);
    int currentDelay = WAIT_DELAY_IN_SECONDS;
    int secondsToWait = MAX_WAIT_SECONDS;
    while (!FINISHED_STATES.contains(importerResponse.getState().toLowerCase())
        && secondsToWait > 0) {
      TimeUnit.SECONDS.sleep(currentDelay);
      secondsToWait -= currentDelay;
      // Exponential backoff with cap
      currentDelay = Math.min(currentDelay * DELAY_MULTIPLIER, MAX_WAIT_DELAY_IN_SECONDS);

      importerResponse = checklistbankWsClient.checkImporter(datasetKey);
    }

    if (!importerResponse.getState().equalsIgnoreCase(FINISHED) && secondsToWait <= 0) {
      throw new IllegalStateException(
          "Max time waiting for api validator response exceeded for key "
              + datasetKey
              + " and archive "
              + archivePath);
    }

    return importerResponse;
  }

  private Optional<String> getFileNameByRowType(int datasetKey, Term rowType) {
    VerbatimResponse verbatimResponse =
        checklistbankWsClient.getVerbatim(datasetKey, rowType.simpleName(), null, 1);

    if (verbatimResponse != null && !verbatimResponse.getResult().isEmpty()) {
      return Optional.ofNullable(verbatimResponse.getResult().get(0).getFile());
    }

    return Optional.empty();
  }

  private List<Metrics.IssueSample> getIssueSamples(int datasetKey, String issue) {
    VerbatimResponse verbatimResponse =
        checklistbankWsClient.getVerbatim(
            datasetKey, DwcTerm.Taxon.simpleName(), issue, SAMPLE_ISSUES_SIZE);
    return verbatimResponse.getResult().stream()
        .map(
            r -> {
              String recordID = r.getTerms().get(DwcTerm.taxonID);
              Map<String, String> relatedData =
                  r.getTerms().entrySet().stream()
                      .filter(t -> t.getKey() != DwcTerm.taxonID)
                      .collect(
                          Collectors.toMap(t -> t.getKey().qualifiedName(), Map.Entry::getValue));
              return Metrics.IssueSample.builder()
                  .recordId(recordID)
                  .relatedData(relatedData)
                  .build();
            })
        .toList();
  }

  private Long getExtensionCount(Term rowType, ImporterResponse importerResponse) {
    Extension extension = Extension.fromRowType(rowType.qualifiedName());
    return switch (extension) {
      case DISTRIBUTION -> importerResponse.getDistributionCount();
      case DESCRIPTION -> importerResponse.getTreatmentCount();
      case REFERENCE -> importerResponse.getReferenceCount();
      case VERNACULAR_NAME -> importerResponse.getVernacularCount();
      case TYPES_AND_SPECIMEN -> importerResponse.getTypeMaterialCount();
      case SPECIES_PROFILE, MEASUREMENT_OR_FACT -> importerResponse.getTaxonCount();
      case MULTIMEDIA -> importerResponse.getMediaCount();
      default -> null;
    };
  }
}
