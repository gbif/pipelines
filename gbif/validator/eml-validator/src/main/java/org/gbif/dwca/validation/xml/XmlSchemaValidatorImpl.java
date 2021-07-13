package org.gbif.dwca.validation.xml;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwca.validation.XmlSchemaValidator;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.api.XmlSchemaValidatorResult.XmlSchemaValidatorError;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

@Slf4j
@Data
@Builder
public class XmlSchemaValidatorImpl implements XmlSchemaValidator {

  private final Schema schema;

  /**
   * Properties XMLConstants.ACCESS_EXTERNAL_DTD and XMLConstants.ACCESS_EXTERNAL_SCHEMA are not
   * supported by Java 8 we currently use
   */
  @SneakyThrows
  private Validator newValidator(Schema schema) {
    Validator validator = schema.newValidator();
    validator.setErrorHandler(new CollectorErrorHandler());
    return validator;
  }

  @SneakyThrows
  @Override
  public XmlSchemaValidatorResult validate(String document) {
    Validator validator = newValidator(schema);
    validator.validate(new StreamSource(new StringReader(document)));
    return XmlSchemaValidatorResult.builder()
        .errors(((CollectorErrorHandler) validator.getErrorHandler()).getErrors())
        .build();
  }

  /**
   * Error handler that collects all possible errors, it only stops when a FATAL error is
   * discovered.
   */
  @Data
  public static class CollectorErrorHandler implements ErrorHandler {

    private final List<XmlSchemaValidatorError> errors = new ArrayList<>();

    @Override
    public void warning(SAXParseException exception) {
      errors.add(
          XmlSchemaValidatorError.builder()
              .level(XmlSchemaValidatorError.Level.WARNING)
              .error(exception.getMessage())
              .build());
    }

    @Override
    public void error(SAXParseException exception) {
      errors.add(
          XmlSchemaValidatorError.builder()
              .level(XmlSchemaValidatorError.Level.ERROR)
              .error(exception.getMessage())
              .build());
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      errors.add(
          XmlSchemaValidatorError.builder()
              .level(XmlSchemaValidatorError.Level.FATAL)
              .error(exception.getMessage())
              .build());
      throw exception;
    }
  }
}
