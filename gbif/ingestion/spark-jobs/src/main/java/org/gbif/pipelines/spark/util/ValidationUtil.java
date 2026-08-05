package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Contract;
import feign.Feign;
import feign.auth.BasicAuthRequestInterceptor;
import feign.httpclient.ApacheHttpClient;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ValidationUtil {

  static ObjectMapper objectMapper = new ObjectMapper();

  public static ValidationClient createValidationClient(PipelinesConfig config) {
    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
    // initialise Validation client to send status updates
    return Feign.builder()
        // Reuse the timeout-configured http client (60s connect/socket) so a slow or
        // unreachable validation service fails instead of blocking the consumer thread.
        .client(new ApacheHttpClient(httpClient))
        .decoder(new JacksonDecoder(objectMapper))
        .encoder(new JacksonEncoder(objectMapper))
        .contract(new Contract.Default())
        .requestInterceptor(
            new BasicAuthRequestInterceptor(
                config.getStandalone().getRegistry().getUser(),
                config.getStandalone().getRegistry().getPassword()))
        .dismiss404()
        .target(ValidationClient.class, config.getStandalone().getRegistry().getWsUrl());
  }

  public static void updateMetrics(
      RetryingValidationClient validationClient, UUID key, Metrics generatedMetrics) {

    Validation validation = validationClient.get(key);
    if (validation == null) {
      log.warn("Can't find validation data key {}, please check that record exists", key);
      return;
    }
    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());

    log.debug("Received file infos {}", metrics.getFileInfos());

    // where possible, update existing fileInfos to preserve filenames and other properties
    // set downstream
    generatedMetrics
        .getFileInfos()
        .forEach(
            generatedFileInfo -> {
              Optional<Metrics.FileInfo> existingFileInfo =
                  metrics.getFileInfos().stream()
                      .filter(
                          f ->
                              f.getRowType() != null
                                  && f.getRowType().equals(generatedFileInfo.getRowType()))
                      .findFirst();
              if (existingFileInfo.isPresent()) {

                Metrics.FileInfo fileInfo = existingFileInfo.get();
                log.info(
                    "Updating metrics for file {}, rowType {}",
                    fileInfo.getFileName(),
                    fileInfo.getRowType());
                fileInfo.setIndexedCount(generatedFileInfo.getIndexedCount());

                // merge the term infos
                mergeTerms(fileInfo, generatedFileInfo);

                // merge issues
                mergeIssues(fileInfo, generatedFileInfo);
              } else {
                log.warn(
                    "Add file info for rowType {} which wasnt found",
                    generatedFileInfo.getRowType());
                metrics.getFileInfos().add(generatedFileInfo);
              }
            });

    // if we have made it to metrics, it should be indexable
    metrics.setIndexeable(true);
    validationClient.update(key, validation);
  }

  private static void mergeTerms(
      Metrics.FileInfo originalFileInfo, Metrics.FileInfo generatedFileInfo) {

    List<Metrics.TermInfo> originalTerms = originalFileInfo.getTerms();
    if (originalTerms == null || originalTerms.isEmpty()) {
      return;
    }

    List<Metrics.TermInfo> mergedTerms = new ArrayList<>(originalTerms);

    Map<Object, Metrics.TermInfo> termsByName =
        originalTerms.stream()
            .collect(Collectors.toMap(Metrics.TermInfo::getTerm, Function.identity()));

    for (Metrics.TermInfo generatedTerm : generatedFileInfo.getTerms()) {
      Metrics.TermInfo existingTerm = termsByName.get(generatedTerm.getTerm());

      if (existingTerm != null) {
        existingTerm.setUniqueRawValues(generatedTerm.getUniqueRawValues());
        existingTerm.setInterpretedIndexed(generatedTerm.getInterpretedIndexed());
        existingTerm.setUniqueInterpretedValues(generatedTerm.getUniqueInterpretedValues());
        existingTerm.setSampleInterpretedValuesMap(generatedTerm.getSampleInterpretedValuesMap());
      } else {
        log.debug("Add term info for term {} which wasn't found", generatedTerm.getTerm());
        mergedTerms.add(generatedTerm);
      }
    }

    originalFileInfo.setTerms(mergedTerms);
  }

  private static void mergeIssues(Metrics.FileInfo fileInfo, Metrics.FileInfo generatedFileInfo) {
    List<Metrics.IssueInfo> mergedIssues = new ArrayList<>(fileInfo.getIssues());

    Set<Object> existingIssues =
        mergedIssues.stream().map(Metrics.IssueInfo::getIssue).collect(Collectors.toSet());

    for (Metrics.IssueInfo generatedIssue : generatedFileInfo.getIssues()) {
      if (existingIssues.add(generatedIssue.getIssue())) {
        log.debug("Add issue info for issue {} which wasn't found", generatedIssue.getIssue());
        mergedIssues.add(generatedIssue);
      }
    }

    fileInfo.setIssues(mergedIssues);
  }
}
