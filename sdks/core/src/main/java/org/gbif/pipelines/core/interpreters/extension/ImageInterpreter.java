package org.gbif.pipelines.core.interpreters.extension;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.MediaParser;
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

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

/**
 * Interpreter for the multimedia extension, Interprets form {@link ExtendedRecord} to {@link ImageRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/images.xml</a>
 */
public class ImageInterpreter {


  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  //
  private static final TargetHandler<Image> HANDLER =
      ExtensionInterpretation.extenstion(Extension.IMAGE)
          .to(Image::new)
          .map(DcTerm.identifier, ImageInterpreter::parseAndsetIdentifier)
          .map(DcTerm.references, ImageInterpreter::parseAndsetReferences)
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

  private ImageInterpreter() {}

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link ImageRecord}
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
   *
   */
  private static void parseAndsetReferences(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(i::setReferences);
  }

  /**
   *
   */
  private static void parseAndsetIdentifier(Image i, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(i::setIdentifier);
  }

  /**
   *
   */
  private static List<String> parseAndSetCreated(Image i, String v) {
    ParsedTemporal parsed = TemporalParser.parse(v);
    parsed.getFrom().map(Temporal::toString).ifPresent(i::setCreated);

    return parsed.getIssueList();
  }

  /**
   *
   */
  private static String parseAndSetLongitude(Image i, String v) {
    Double lat = NumberParser.parseDouble(v);
    Optional.ofNullable(lat).ifPresent(i::setLatitude);
    return lat == null ? COORDINATE_INVALID.name() : null;
  }

  /**
   *
   */
  private static String parseAndSetLatitude(Image i, String v) {
    Double lng = NumberParser.parseDouble(v);
    Optional.ofNullable(lng).ifPresent(i::setLongitude);
    return lng == null ? COORDINATE_INVALID.name() : null;
  }

  /**
   *
   */
  private static List<String> parseAndSetLatLng(Image i) {
    String lat = i.getLatitude().toString();
    String lng = i.getLongitude().toString();
    ParsedField<LatLng> latLng = CoordinateParseUtils.parseLatLng(lat, lng);
    if (latLng.isSuccessful()) {
      LatLng result = latLng.getResult();
      i.setLatitude(result.getLatitude());
      i.setLongitude(result.getLongitude());
    }
    return latLng.getIssues();
  }

  /**
   *
   */
  private static Optional<String> checkLinks(Image i) {
    if (i.getReferences() == null && i.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }

}
