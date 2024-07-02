package org.gbif.pipelines.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Validation;
import org.junit.Test;

public class ValidationsTest {

  @Test
  public void nullTest() {
    // State
    Validation validation = null;
    FileInfo fileInfo = null;

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertNull(validation);
    assertNull(fileInfo);
  }

  @Test
  public void nullMetricsTest() {
    // State
    Validation validation = Validation.builder().build();
    FileInfo fileInfo = FileInfo.builder().build();

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertEquals(1, validation.getMetrics().getFileInfos().size());
  }

  @Test
  public void emptyFileInfosTest() {
    // State
    Validation validation = Validation.builder().metrics(Metrics.builder().build()).build();
    FileInfo fileInfo = FileInfo.builder().build();

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertEquals(1, validation.getMetrics().getFileInfos().size());
  }

  @Test
  public void mergeFileInfosNoTypeTest() {
    // State
    Validation validation =
        Validation.builder()
            .metrics(
                Metrics.builder()
                    .fileInfos(
                        List.of(
                            FileInfo.builder()
                                .fileName("ONE")
                                .issues(List.of(IssueInfo.builder().issue("ONE").build()))
                                .build()))
                    .build())
            .build();

    FileInfo fileInfo =
        FileInfo.builder()
            .fileName("TWO")
            .issues(List.of(IssueInfo.builder().issue("TWO").build()))
            .build();

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertEquals(2, validation.getMetrics().getFileInfos().size());
  }

  @Test
  public void mergeileInfosTest() {
    // State
    Validation validation =
        Validation.builder()
            .metrics(
                Metrics.builder()
                    .fileInfos(
                        List.of(
                            FileInfo.builder()
                                .fileName("ONE")
                                .fileType(DwcFileType.CORE)
                                .issues(List.of(IssueInfo.builder().issue("ONE").build()))
                                .build()))
                    .build())
            .build();

    FileInfo fileInfo =
        FileInfo.builder()
            .fileName("ONE")
            .fileType(DwcFileType.CORE)
            .issues(List.of(IssueInfo.builder().issue("TWO").build()))
            .build();

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertEquals(1, validation.getMetrics().getFileInfos().size());

    FileInfo info = validation.getMetrics().getFileInfos().get(0);
    assertEquals("ONE", info.getFileName());
    assertEquals(2, info.getIssues().size());
    assertEquals(0, info.getTerms().size());
  }

  @Test
  public void mergeFileInfosDiffTypesTest() {
    // State
    Validation validation =
        Validation.builder()
            .metrics(
                Metrics.builder()
                    .fileInfos(
                        List.of(
                            FileInfo.builder()
                                .fileName("ONE")
                                .fileType(DwcFileType.CORE)
                                .issues(List.of(IssueInfo.builder().issue("ONE").build()))
                                .build()))
                    .build())
            .build();

    FileInfo fileInfo =
        FileInfo.builder()
            .fileName("TWO")
            .fileType(DwcFileType.METADATA)
            .issues(List.of(IssueInfo.builder().issue("TWO").build()))
            .build();

    // When
    Validations.mergeFileInfo(validation, fileInfo);

    // Should
    assertEquals(2, validation.getMetrics().getFileInfos().size());

    assertEquals(1, validation.getMetrics().getFileInfos().get(0).getIssues().size());
    assertEquals(1, validation.getMetrics().getFileInfos().get(1).getIssues().size());
  }
}
