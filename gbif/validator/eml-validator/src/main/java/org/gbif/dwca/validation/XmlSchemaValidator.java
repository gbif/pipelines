package org.gbif.dwca.validation;

import org.gbif.validator.api.XmlSchemaValidatorResult;

public interface XmlSchemaValidator {

  XmlSchemaValidatorResult validate(String document);
}
