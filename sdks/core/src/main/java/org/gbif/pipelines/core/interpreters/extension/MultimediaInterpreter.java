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
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

/**
 * Interpreter for the multimedia extension, Interprets form {@link ExtendedRecord} to {@link MultimediaRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/multimedia.xml</a>
 */
public class MultimediaInterpreter {

  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  //
  private static final TargetHandler<Multimedia> HANDLER =
      ExtensionInterpretation.extenstion(Extension.MULTIMEDIA)
          .to(Multimedia::new)
          .map(DcTerm.references, MultimediaInterpreter::parseAndsetReferences)
          .map(DcTerm.identifier, MultimediaInterpreter::parseAndsetIdentifier)
          .map(DcTerm.type, MultimediaInterpreter::parseAndSetType)
          .map(DcTerm.format, MultimediaInterpreter::parseAndSetFormat)
          .map(DcTerm.created, MultimediaInterpreter::parseAndSetCreated)
          .map(DcTerm.title, Multimedia::setTitle)
          .map(DcTerm.description, Multimedia::setDescription)
          .map(DcTerm.contributor, Multimedia::setContributor)
          .map(DcTerm.publisher, Multimedia::setPublisher)
          .map(DcTerm.audience, Multimedia::setAudience)
          .map(DcTerm.creator, Multimedia::setCreator)
          .map(DcTerm.license, Multimedia::setLicense)
          .map(DcTerm.rightsHolder, Multimedia::setRightsHolder)
          .map(DcTerm.source, Multimedia::setSource)
          .map(DwcTerm.datasetID, Multimedia::setDatasetId)
          .skipIf(MultimediaInterpreter::checkLinks);

  private MultimediaInterpreter() {}

  /**
   * Interprets the multimedia of a {@link ExtendedRecord} and populates a {@link MultimediaRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, MultimediaRecord mr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mr);

    Result<Multimedia> result = HANDLER.convert(er);

    mr.setMultimediaItems(result.getList());
    mr.getIssues().setIssueList(result.getIssuesAsList());
  }

  /**
   *
   */
  private static void parseAndsetReferences(Multimedia m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setReferences);
  }

  /**
   *
   */
  private static void parseAndsetIdentifier(Multimedia m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setIdentifier);
  }

  /**
   *
   */
  private static void parseAndSetType(Multimedia m, String v) {
    if (!Strings.isNullOrEmpty(v)) {
      if (v.toLowerCase().startsWith("image")) {
        m.setType(MediaType.StillImage.name());
      } else if (v.toLowerCase().startsWith("audio")) {
        m.setType(MediaType.Sound.name());
      } else if (v.toLowerCase().startsWith("video")) {
        m.setType(MediaType.MovingImage.name());
      }
    }
  }

  /**
   *
   */
  private static List<String> parseAndSetCreated(Multimedia m, String v) {
    ParsedTemporal parsed = TemporalParser.parse(v);
    parsed.getFrom().map(Temporal::toString).ifPresent(m::setCreated);

    return parsed.getIssueList();
  }

  /**
   *
   */
  private static void parseAndSetFormat(Multimedia m, String v) {
    String mimeType = MEDIA_PARSER.parseMimeType(v);
    if (Strings.isNullOrEmpty(mimeType)) {
      mimeType = MEDIA_PARSER.parseMimeType(m.getIdentifier());
    }
    m.setFormat(mimeType);
  }

  /**
   *
   */
  private static Optional<String> checkLinks(Multimedia m) {
    if (m.getReferences() == null && m.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }
}
