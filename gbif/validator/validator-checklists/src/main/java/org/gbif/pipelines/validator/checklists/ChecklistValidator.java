package org.gbif.pipelines.validator.checklists;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.validator.checklists.ws.ChecklistbankWsClient;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/** Evaluates checklists using ChecklistBank.org API. */
@Slf4j
public class ChecklistValidator {

  private static final int SAMPLE_ISSUES_SIZE = 5;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ChecklistbankWsClient checklistbankWsClient;
  private final String callbackUrl;

  public ChecklistValidator(
      String clbApiUrl, String clbApiUser, String clbApiPassword, String callbackUrl) {
    this(buildChecklistbankWsClient(clbApiUrl, clbApiUser, clbApiPassword), callbackUrl);
  }

  public ChecklistValidator(ChecklistbankWsClient checklistbankWsClient, String callbackUrl) {
    this.checklistbankWsClient = checklistbankWsClient;
    this.callbackUrl = callbackUrl;
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

  public CompletableFuture<Integer> submitValidation(Path archivePath, UUID validationKey) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            ChecklistbankWsClient.ValidatorResponse validatorResponse =
                checklistbankWsClient.validateArchive(
                    callbackUrl + "/" + validationKey, Files.readAllBytes(archivePath));

            int datasetKey = validatorResponse.getKey();
            if (datasetKey == 0) {
              throw new IllegalStateException(
                  "Validation failed with key zero for "
                      + archivePath
                      + ". Most likely is that the CLB API service is off.");
            }

            return datasetKey;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @SneakyThrows
  public List<Metrics.FileInfo> evaluateResults(ClbDatasetImport clbDatasetImport) {
    List<Metrics.FileInfo> results = new ArrayList<>();

    int datasetKey = clbDatasetImport.getDatasetKey();

    for (Map.Entry<Term, Map<Term, Long>> entry :
        clbDatasetImport.getVerbatimByRowTypeCount().entrySet()) {
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
            clbDatasetImport.getIssuesCount().entrySet().stream()
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
                .count(clbDatasetImport.getVerbatimByTermCount().get(rowType))
                .fileName(getFileNameByRowType(datasetKey, rowType).orElse(null))
                .fileType(DwcFileType.CORE)
                .issues(issues)
                .terms(termsInfo)
                .indexedCount(clbDatasetImport.getUsagesCount())
                .build());
      } else {
        // extensions
        results.add(
            Metrics.FileInfo.builder()
                .rowType(rowType.qualifiedName())
                .count(clbDatasetImport.getVerbatimByTermCount().get(rowType))
                .fileName(getFileNameByRowType(datasetKey, rowType).orElse(null))
                .fileType(DwcFileType.EXTENSION)
                .terms(termsInfo)
                .indexedCount(getExtensionCount(rowType, clbDatasetImport))
                .build());
      }
    }

    return results;
  }

  @SneakyThrows
  public PipelinesBalancerMessage createNextMessage(String rawPreviousMessage) {
    PipelinesArchiveValidatorMessage previousMessage =
        OBJECT_MAPPER.readValue(rawPreviousMessage, PipelinesArchiveValidatorMessage.class);

    PipelinesDwcaMessage nextMessage = new PipelinesDwcaMessage();
    nextMessage.setDatasetUuid(previousMessage.getDatasetUuid());
    nextMessage.setAttempt(previousMessage.getAttempt());
    nextMessage.setValidationReport(
        new DwcaValidationReport(
            previousMessage.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)));
    nextMessage.setPipelineSteps(previousMessage.getPipelineSteps());
    nextMessage.setExecutionId(previousMessage.getExecutionId());
    nextMessage.setDatasetType(DatasetType.CHECKLIST);
    nextMessage.setEndpointType(EndpointType.DWC_ARCHIVE);

    String nextMessageClassName = nextMessage.getClass().getSimpleName();
    String messagePayload = nextMessage.toString();
    return new PipelinesBalancerMessage(nextMessageClassName, messagePayload);
  }

  private Optional<String> getFileNameByRowType(int datasetKey, Term rowType) {
    ChecklistbankWsClient.VerbatimResponse verbatimResponse =
        checklistbankWsClient.getVerbatim(datasetKey, rowType.simpleName(), null, 1);

    if (verbatimResponse != null && !verbatimResponse.getResult().isEmpty()) {
      return Optional.ofNullable(verbatimResponse.getResult().get(0).getFile());
    }

    return Optional.empty();
  }

  private List<Metrics.IssueSample> getIssueSamples(int datasetKey, String issue) {
    ChecklistbankWsClient.VerbatimResponse verbatimResponse =
        checklistbankWsClient.getVerbatim(
            datasetKey, DwcTerm.Taxon.simpleName(), issue, SAMPLE_ISSUES_SIZE);
    if (verbatimResponse == null
        || verbatimResponse.getResult() == null
        || verbatimResponse.getResult().isEmpty()) {
      log.warn("No samples found for issue {} in dataset {}.", issue, datasetKey);
      return List.of();
    }

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

  private Long getExtensionCount(Term rowType, ClbDatasetImport importerResponse) {
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
