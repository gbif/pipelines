package org.gbif.pipelines.validator.rules;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.model.registry.CitationContact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.util.CitationGenerator;
import org.gbif.metadata.eml.parse.DatasetEmlParser;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics.IssueInfo;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BasicMetadataEvaluator {

  private static final int MIN_TITLE_LENGTH = 10;
  private static final int MIN_DESCRIPTION_LENGTH = 25;

  public static List<IssueInfo> evaluate(String eml) {
    List<IssueInfo> issues = new ArrayList<>();
    try {
      Dataset dataset = DatasetEmlParser.build(eml.getBytes(StandardCharsets.UTF_8));
      evaluateTitle(dataset).ifPresent(issues::add);
      evaluateLicense(dataset).ifPresent(issues::add);
      evaluateDescription(dataset).ifPresent(issues::add);
      evaluateContact(dataset).ifPresent(issues::add);
    } catch (Exception ex) {
      log.error("Can't deserialize xml string to an object");
      issues.add(IssueInfo.create(EvaluationType.EML_GBIF_SCHEMA));
    }
    return issues;
  }

  private static Optional<IssueInfo> evaluateTitle(Dataset dataset) {
    if (StringUtils.isBlank(dataset.getTitle()) || dataset.getTitle().length() < MIN_TITLE_LENGTH) {
      return Optional.of(IssueInfo.create(EvaluationType.TITLE_MISSING_OR_TOO_SHORT));
    }
    return Optional.empty();
  }

  private static Optional<IssueInfo> evaluateLicense(Dataset dataset) {
    if (dataset.getLicense() == null) {
      return Optional.of(IssueInfo.create(EvaluationType.LICENSE_MISSING_OR_UNKNOWN));
    }
    return Optional.empty();
  }

  private static Optional<IssueInfo> evaluateDescription(Dataset dataset) {
    if (StringUtils.isBlank(dataset.getDescription())
        || dataset.getDescription().length() < MIN_DESCRIPTION_LENGTH) {
      return Optional.of(IssueInfo.create(EvaluationType.DESCRIPTION_MISSING_OR_TOO_SHORT));
    }
    return Optional.empty();
  }

  private static Optional<IssueInfo> evaluateContact(Dataset dataset) {
    List<CitationContact> authorList = CitationGenerator.getAuthors(dataset.getContacts());
    // we want at least 1 author and for ALL authors it must be possible to generate an author name
    if (authorList.isEmpty()
        || authorList.size() != CitationGenerator.generateAuthorsName(authorList).size()) {
      return Optional.of(IssueInfo.create(EvaluationType.RESOURCE_CONTACTS_MISSING_OR_INCOMPLETE));
    }
    return Optional.empty();
  }
}
