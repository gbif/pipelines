package org.gbif.pipelines.validator.rules;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class BasicMetadataEvaluatorTest {

  @Test
  public void allIssuesTest() throws Exception {
    // State
    String file = this.getClass().getResource("/dwca/eml-content-issues.xml").getFile();
    String eml = new String(Files.readAllBytes(Paths.get(file)));
    // When
    List<IssueInfo> result = BasicMetadataEvaluator.evaluate(eml);

    // Should
    assertType(EvaluationType.LICENSE_MISSING_OR_UNKNOWN, result);
    assertType(EvaluationType.TITLE_MISSING_OR_TOO_SHORT, result);
    assertType(EvaluationType.DESCRIPTION_MISSING_OR_TOO_SHORT, result);
    assertType(EvaluationType.RESOURCE_CONTACTS_MISSING_OR_INCOMPLETE, result);
  }

  @Test
  public void invalidEmlXmlTest() {
    // State
    String eml = "null";
    // When
    List<IssueInfo> result = BasicMetadataEvaluator.evaluate(eml);

    // Should
    assertType(EvaluationType.EML_GBIF_SCHEMA, result);
  }

  private void assertType(EvaluationType expected, List<IssueInfo> result) {
    IssueInfo info =
        result.stream().filter(x -> x.getIssue().equals(expected.name())).findFirst().orElse(null);
    Assert.equals(expected.name(), info.getIssue());
  }
}
