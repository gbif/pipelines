package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptListValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 * it. TODO: REFACTOR AND MERGE WITH BasicTransform
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventCoreInterpreter {

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(ExtendedRecord er, EventCoreRecord br) {
    String value = extractValue(er, DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        br.setReferences(parseResult.toString());
      } else {
        addIssue(br, REFERENCES_URI_INVALID);
      }
    }
  }

  /** {@link DwcTerm#sampleSizeValue} interpretation. */
  public static void interpretSampleSizeValue(ExtendedRecord er, EventCoreRecord br) {
    extractOptValue(er, DwcTerm.sampleSizeValue)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(br::setSampleSizeValue);
  }

  /** {@link DwcTerm#sampleSizeUnit} interpretation. */
  public static void interpretSampleSizeUnit(ExtendedRecord er, EventCoreRecord br) {
    extractOptValue(er, DwcTerm.sampleSizeUnit).map(String::trim).ifPresent(br::setSampleSizeUnit);
  }

  /** {@link DcTerm#license} interpretation. */
  public static void interpretLicense(ExtendedRecord er, EventCoreRecord br) {
    String license =
        extractOptValue(er, DcTerm.license)
            .map(EventCoreInterpreter::getLicense)
            .map(License::name)
            .orElse(License.UNSPECIFIED.name());

    br.setLicense(license);
  }

  /** {@link DwcTerm#datasetID} interpretation. */
  public static void interpretDatasetID(ExtendedRecord er, EventCoreRecord br) {
    extractOptListValue(er, DwcTerm.datasetID).ifPresent(br::setDatasetID);
  }

  /** {@link DwcTerm#datasetName} interpretation. */
  public static void interpretDatasetName(ExtendedRecord er, EventCoreRecord br) {
    extractOptListValue(er, DwcTerm.datasetName).ifPresent(br::setDatasetName);
  }

  /** {@link DwcTerm#samplingProtocol} interpretation. */
  public static void interpretSamplingProtocol(ExtendedRecord er, EventCoreRecord br) {
    extractOptListValue(er, DwcTerm.samplingProtocol).ifPresent(br::setSamplingProtocol);
  }

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }
}
