package org.gbif.pipelines.core.interpreters.extension;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

import com.google.common.base.Strings;
import java.net.URI;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.LicenseUriParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.MultiinputTemporalParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.CoordinateParseUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;

/**
 * Interpreter for the Image extension, Interprets form {@link ExtendedRecord} to {@link
 * ImageRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/images.xml</a>
 */
public class ImageInterpreter {

  private static final LicenseUriParser LICENSE_URI_PARSER = LicenseUriParser.getInstance();

  private final TargetHandler<Image> handler =
      ExtensionInterpretation.extension(Extension.IMAGE)
          .to(Image::new)
          .mapOne(DcTerm.identifier, ImageInterpreter::parseAndSetIdentifier)
          .map(DcTerm.references, ImageInterpreter::parseAndSetReferences)
          .mapOne(DcTerm.created, this::parseAndSetCreated)
          .map(DcTerm.license, ImageInterpreter::parseAndSetLicense)
          .map(DcTerm.title, Image::setTitle)
          .map(DcTerm.description, Image::setDescription)
          .map(DcTerm.spatial, Image::setSpatial)
          .map(DcTerm.format, Image::setFormat)
          .map(DcTerm.creator, Image::setCreator)
          .map(DcTerm.contributor, Image::setContributor)
          .map(DcTerm.publisher, Image::setPublisher)
          .map(DcTerm.audience, Image::setAudience)
          .map(DcTerm.rightsHolder, Image::setRightsHolder)
          .map(DwcTerm.datasetID, Image::setDatasetId)
          .map(
              "http://www.w3.org/2003/01/geo/wgs84_pos#longitude",
              ImageInterpreter::parseAndSetLongitude)
          .map(
              "http://www.w3.org/2003/01/geo/wgs84_pos#latitude",
              ImageInterpreter::parseAndSetLatitude)
          .postMap(ImageInterpreter::parseAndSetLatLng)
          .skipIf(ImageInterpreter::checkLinks);

  private final MultiinputTemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private ImageInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.temporalParser = MultiinputTemporalParser.create(orderings);
    this.preprocessDateFn = preprocessDateFn;
  }

  /**
   * Interprets images of a {@link ExtendedRecord} and populates a {@link ImageRecord} with the
   * interpreted values.
   */
  public void interpret(ExtendedRecord er, ImageRecord mr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mr);

    Result<Image> result = handler.convert(er);

    mr.setImageItems(result.getList());
    mr.getIssues().setIssueList(result.getIssuesAsList());
  }

  /** Parser for "http://purl.org/dc/terms/references" term value */
  private static void parseAndSetReferences(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(i::setReferences);
  }

  /** Parser for "http://purl.org/dc/terms/identifier" term value */
  private static String parseAndSetIdentifier(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional<URI> uriOpt = Optional.ofNullable(uri);
    if (uriOpt.isPresent()) {
      Optional<String> opt = uriOpt.map(URI::toString);
      if (opt.isPresent()) {
        opt.ifPresent(i::setIdentifier);
      } else {
        return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
      }
    } else {
      return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
    }
    return "";
  }

  /** Parser for "http://www.w3.org/2003/01/geo/wgs84_pos#longitude" term value */
  private static void parseAndSetLongitude(Image i, String v) {
    if (!Strings.isNullOrEmpty(v)) {
      Double lat = NumberParser.parseDouble(v);
      Optional.ofNullable(lat).ifPresent(i::setLatitude);
    }
  }

  /** Parser for "http://www.w3.org/2003/01/geo/wgs84_pos#latitude" term value */
  private static void parseAndSetLatitude(Image i, String v) {
    if (!Strings.isNullOrEmpty(v)) {
      Double lng = NumberParser.parseDouble(v);
      Optional.ofNullable(lng).ifPresent(i::setLongitude);
    }
  }

  /** Parse and check coordinates */
  private static void parseAndSetLatLng(Image i) {
    if (i.getLatitude() != null && i.getLongitude() != null) {
      String lat = Optional.ofNullable(i.getLatitude()).map(Object::toString).orElse(null);
      String lng = Optional.ofNullable(i.getLongitude()).map(Object::toString).orElse(null);

      ParsedField<GeocodeRequest> latLng = CoordinateParseUtils.parseLatLng(lat, lng);
      if (latLng.isSuccessful()) {
        GeocodeRequest result = latLng.getResult();
        i.setLatitude(result.getLat());
        i.setLongitude(result.getLng());
      }
    }
  }

  /** Returns ENUM instead of url string */
  private static void parseAndSetLicense(Image i, String v) {
    if (Objects.nonNull(v)) {
      ParseResult<URI> parsed = LICENSE_URI_PARSER.parse(v);
      i.setLicense(parsed.isSuccessful() ? parsed.getPayload().toString() : v);
    }
  }

  /** Skip whole record if both links are absent */
  private static Optional<String> checkLinks(Image i) {
    if (i.getReferences() == null && i.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }

  /** Parser for "http://purl.org/dc/terms/created" term value */
  private String parseAndSetCreated(Image i, String v) {
    String normalizedDate = Optional.ofNullable(preprocessDateFn).map(x -> x.apply(v)).orElse(v);
    OccurrenceParseResult<TemporalAccessor> result =
        temporalParser.parseRecordedDate(normalizedDate);
    if (result.isSuccessful()) {
      i.setCreated(result.getPayload().toString());
      return "";
    } else {
      return MULTIMEDIA_DATE_INVALID.name();
    }
  }
}
