package org.gbif.pipelines.tasks;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.Assert;
import org.junit.Test;

public class ValidationsTest {

  @Test
  public void allPriviousShouldFinishedTest() {
    UUID uuid = UUID.randomUUID();

    Validation validation =
        Validation.builder()
            .key(uuid)
            .metrics(
                Metrics.builder()
                    .stepTypes(
                        new ArrayList<>(
                            Arrays.asList(
                                ValidationStep.builder()
                                    .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE.name())
                                    .status(Status.FINISHED)
                                    .build(),
                                ValidationStep.builder()
                                    .stepType(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name())
                                    .status(Status.RUNNING)
                                    .build())))
                    .fileInfos(
                        Collections.singletonList(
                            FileInfo.builder()
                                .fileName("verbatim.txt")
                                .fileType(DwcFileType.CORE)
                                .issues(
                                    Collections.singletonList(
                                        IssueInfo.builder().issue("OLD").count(999L).build()))
                                .build()))
                    .build())
            .build();

    ValidationWsClientStub wsClientStub = ValidationWsClientStub.create(validation);
    StepType stepType = StepType.VALIDATOR_COLLECT_METRICS;
    Status status = Status.FINISHED;

    Validations.updateStatus(wsClientStub, uuid, stepType, status);

    Validation result = wsClientStub.get(uuid);
    Assert.assertTrue(
        result.getMetrics().getStepTypes().stream()
            .allMatch(x -> x.getStatus() == Status.FINISHED));
  }

  @Test
  public void allPriviousShouldFinishedOneTest() {
    UUID uuid = UUID.randomUUID();

    Validation validation =
        Validation.builder()
            .key(uuid)
            .metrics(
                Metrics.builder()
                    .stepTypes(
                        new ArrayList<>(
                            Arrays.asList(
                                ValidationStep.builder()
                                    .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE.name())
                                    .status(Status.FINISHED)
                                    .build(),
                                ValidationStep.builder()
                                    .stepType(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name())
                                    .status(Status.RUNNING)
                                    .build())))
                    .fileInfos(
                        Collections.singletonList(
                            FileInfo.builder()
                                .fileName("verbatim.txt")
                                .fileType(DwcFileType.CORE)
                                .issues(
                                    Collections.singletonList(
                                        IssueInfo.builder().issue("OLD").count(999L).build()))
                                .build()))
                    .build())
            .build();

    ValidationWsClientStub wsClientStub = ValidationWsClientStub.create(validation);
    StepType stepType = StepType.VALIDATOR_COLLECT_METRICS;
    Status status = Status.FAILED;

    Validations.updateStatus(wsClientStub, uuid, stepType, status);

    Validation result = wsClientStub.get(uuid);
    Assert.assertEquals(
        2,
        result.getMetrics().getStepTypes().stream()
            .filter(x -> x.getStatus() == Status.FINISHED)
            .count());
    Assert.assertEquals(
        1,
        result.getMetrics().getStepTypes().stream()
            .filter(x -> x.getStatus() == Status.FAILED)
            .count());
  }

  @AllArgsConstructor(staticName = "create")
  private static class ValidationWsClientStub implements ValidationWsClient {

    private final Validation validation;

    @Override
    public boolean reachedMaxRunningValidations(String userName) {
      return false;
    }

    @Override
    public Validation submitFile(File file) {
      return null;
    }

    @Override
    public Validation validateFile(File file, ValidationRequest validationRequest) {
      return null;
    }

    @Override
    public Validation validateFileFromUrl(String fileUrl, ValidationRequest validationRequest) {
      return null;
    }

    @Override
    public PagingResponse<Validation> list(Map<String, Object> validationSearchRequest) {
      return null;
    }

    @Override
    public Validation get(UUID key) {
      if (validation.getKey().equals(key)) {
        return validation;
      }
      return null;
    }

    @Override
    public Validation update(UUID key, Validation validation) {
      return null;
    }

    @Override
    public Validation cancel(UUID key) {
      return null;
    }

    @Override
    public void delete(UUID key) {}

    @Override
    public Dataset getDataset(UUID key) {
      return null;
    }
  }
}
