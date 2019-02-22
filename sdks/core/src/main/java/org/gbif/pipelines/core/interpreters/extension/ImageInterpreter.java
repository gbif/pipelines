package org.gbif.pipelines.core.interpreters.extension;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import com.google.common.base.Strings;

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
      ExtensionInterpretation.extenstion(Extension.MULTIMEDIA)
          .to(Image::new)
          .map(DcTerm.references, ImageInterpreter::parseAndsetReferences)
          .map(DcTerm.identifier, ImageInterpreter::parseAndsetIdentifier)
          .map(DcTerm.format, ImageInterpreter::parseAndSetFormat)
          .map(DcTerm.created, ImageInterpreter::parseAndSetCreated)
          .map(DcTerm.title, Image::setTitle)
          .map(DcTerm.description, Image::setDescription)
          .map(DcTerm.contributor, Image::setContributor)
          .map(DcTerm.publisher, Image::setPublisher)
          .map(DcTerm.audience, Image::setAudience)
          .map(DcTerm.creator, Image::setCreator)
          .map(DcTerm.license, Image::setLicense)
          .map(DcTerm.rightsHolder, Image::setRightsHolder)
          .map(DwcTerm.datasetID, Image::setDatasetId)
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
  private static void parseAndsetReferences(Image m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setReferences);
  }

  /**
   *
   */
  private static void parseAndsetIdentifier(Image m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setIdentifier);
  }

  /**
   *
   */
  private static List<String> parseAndSetCreated(Image m, String v) {
    ParsedTemporal parsed = TemporalParser.parse(v);
    parsed.getFrom().map(Temporal::toString).ifPresent(m::setCreated);

    return parsed.getIssueList();
  }

  /**
   *
   */
  private static void parseAndSetFormat(Image m, String v) {
    String mimeType = MEDIA_PARSER.parseMimeType(v);
    if (Strings.isNullOrEmpty(mimeType)) {
      mimeType = MEDIA_PARSER.parseMimeType(m.getIdentifier());
    }
    m.setFormat(mimeType);
  }

  /**
   *
   */
  private static Optional<String> checkLinks(Image m) {
    if (m.getReferences() == null && m.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }

}
