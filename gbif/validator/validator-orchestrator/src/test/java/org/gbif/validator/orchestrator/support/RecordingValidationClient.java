package org.gbif.validator.orchestrator.support;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Reusable recording fake for validator-ws integration tests. */
public class RecordingValidationClient implements ValidationWsClient {

  private Validation validation;
  private int statusWrites;
  private int reportWrites;
  private UUID reportKey;
  private int reportAttempt;
  private String reportType;
  private String reportJson;
  private boolean rejectReport;

  public void setValidation(Validation validation) {
    this.validation = validation;
  }

  public Validation getStoredValidation() {
    return validation;
  }

  public int getStatusWrites() {
    return statusWrites;
  }

  public int getReportWrites() {
    return reportWrites;
  }

  public UUID getReportKey() {
    return reportKey;
  }

  public int getReportAttempt() {
    return reportAttempt;
  }

  public String getReportType() {
    return reportType;
  }

  public String getReportJson() {
    return reportJson;
  }

  public void setRejectReport(boolean rejectReport) {
    this.rejectReport = rejectReport;
  }

  @Override
  public void createOrUpdateValidationReport(
      UUID key, int attempt, String reportType, String report) {
    if (rejectReport) {
      throw new IllegalStateException("Report endpoint unavailable");
    }

    reportWrites++;
    reportKey = key;
    reportAttempt = attempt;
    this.reportType = reportType;
    reportJson = report;
  }

  @Override
  public String getValidationReport(UUID key, int attempt, String reportType) {
    throw new UnsupportedOperationException("getValidationReport");
  }

  @Override
  public Validation get(UUID key) {
    return validation;
  }

  @Override
  public Validation update(UUID key, Validation validation) {
    this.validation = validation;
    statusWrites++;
    return validation;
  }

  @Override
  public boolean reachedMaxRunningValidations(String userName) {
    return false;
  }

  @Override
  public void validateChecklistResults(UUID validationKey, ClbDatasetImport clbDatasetImport) {
    throw new UnsupportedOperationException("validateChecklistResults");
  }

  @Override
  public Validation validateFile(File file, ValidationRequest request) {
    throw new UnsupportedOperationException("validateFile");
  }

  @Override
  public Validation validateFileFromUrl(String url, ValidationRequest request) {
    throw new UnsupportedOperationException("validateFileFromUrl");
  }

  @Override
  public PagingResponse<Validation> list(Map<String, Object> request) {
    throw new UnsupportedOperationException("list");
  }

  @Override
  public Validation cancel(UUID key) {
    throw new UnsupportedOperationException("cancel");
  }

  @Override
  public void delete(UUID key) {
    throw new UnsupportedOperationException("delete");
  }

  @Override
  public Dataset getDataset(UUID key) {
    throw new UnsupportedOperationException("getDataset");
  }

  @Override
  public List<UUID> getRunningValidations(int min) {
    throw new UnsupportedOperationException("getRunningValidations");
  }
}
