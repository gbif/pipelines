package org.gbif.pipelines.core.interpreters.extension;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.location.legacy.CoordinateParseUtils;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

/**
 * Interpreter for the Image extension, Interprets form {@link ExtendedRecord} to {@link ImageRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/images.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ImageInterpreter {

  private static final TargetHandler<Image> HANDLER =
      ExtensionInterpretation.extension(Extension.IMAGE)
          .to(Image::new)
          .map(DcTerm.identifier, ImageInterpreter::parseAndSetIdentifier)
          .map(DcTerm.references, ImageInterpreter::parseAndSetReferences)
          .map(DcTerm.created, ImageInterpreter::parseAndSetCreated)
          .map(DcTerm.title, Image::setTitle)
          .map(DcTerm.description, Image::setDescription)
          .map(DcTerm.spatial, Image::setSpatial)
          .map(DcTerm.format, Image::setFormat)
          .map(DcTerm.creator, Image::setCreator)
          .map(DcTerm.contributor, Image::setContributor)
          .map(DcTerm.publisher, Image::setPublisher)
          .map(DcTerm.audience, Image::setAudience)
          .map(DcTerm.license, Image::setLicense)
          .map(DcTerm.rightsHolder, Image::setRightsHolder)
          .map(DwcTerm.datasetID, Image::setDatasetId)
          .mapOne("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", ImageInterpreter::parseAndSetLongitude)
          .mapOne("http://www.w3.org/2003/01/geo/wgs84_pos#latitude", ImageInterpreter::parseAndSetLatitude)
          .postMap(ImageInterpreter::parseAndSetLatLng)
          .skipIf(ImageInterpreter::checkLinks);

  /**
   * Interprets images of a {@link ExtendedRecord} and populates a {@link ImageRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, ImageRecord mr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mr);

    Result<Image> result = HANDLER.convert(er);

    mr.setImageItems(result.getList());
    mr.getIssues().setIssueList(result.getIssuesAsList());
  }

  /**
   * Parser for "http://purl.org/dc/terms/references" term value
   */
  private static void parseAndSetReferences(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(i::setReferences);
  }

  /**
   * Parser for "http://purl.org/dc/terms/identifier" term value
   */
  private static void parseAndSetIdentifier(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(i::setIdentifier);
  }

  /**
   * Parser for "http://purl.org/dc/terms/created" term value
   */
  private static List<String> parseAndSetCreated(Image i, String v) {
    ParsedTemporal parsed = TemporalParser.parse(v);
    parsed.getFrom().map(Temporal::toString).ifPresent(i::setCreated);

    return parsed.getIssueList();
  }

  /**
   * Parser for "http://www.w3.org/2003/01/geo/wgs84_pos#longitude" term value
   */
  private static String parseAndSetLongitude(Image i, String v) {
    Double lat = NumberParser.parseDouble(v);
    Optional.ofNullable(lat).ifPresent(i::setLatitude);
    return lat == null ? COORDINATE_INVALID.name() : null;
  }

  /**
   * Parser for "http://www.w3.org/2003/01/geo/wgs84_pos#latitude" term value
   */
  private static String parseAndSetLatitude(Image i, String v) {
    Double lng = NumberParser.parseDouble(v);
    Optional.ofNullable(lng).ifPresent(i::setLongitude);
    return lng == null ? COORDINATE_INVALID.name() : null;
  }

  /**
   * Parse and check coordinates
   */
  private static List<String> parseAndSetLatLng(Image i) {

    if (i.getLatitude() == null && i.getLongitude() == null) {
      return Collections.emptyList();
    }

    String lat = Optional.ofNullable(i.getLatitude()).map(Object::toString).orElse(null);
    String lng = Optional.ofNullable(i.getLongitude()).map(Object::toString).orElse(null);

    ParsedField<LatLng> latLng = CoordinateParseUtils.parseLatLng(lat, lng);
    if (latLng.isSuccessful()) {
      LatLng result = latLng.getResult();
      i.setLatitude(result.getLatitude());
      i.setLongitude(result.getLongitude());
    }
    return latLng.getIssues();
  }

  /**
   * Skip whole record if both links are absent
   */
  private static Optional<String> checkLinks(Image i) {
    if (i.getReferences() == null && i.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }

}
