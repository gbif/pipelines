package org.gbif.dwca.validation;

import java.util.List;
import org.gbif.validator.api.Metrics.IssueInfo;

public interface XmlSchemaValidator {

  List<IssueInfo> validate(String document);
}
