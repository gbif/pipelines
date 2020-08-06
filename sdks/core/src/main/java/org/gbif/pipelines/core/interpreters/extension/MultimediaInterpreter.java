package org.gbif.pipelines.core.interpreters.extension;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import com.google.common.base.Strings;
import java.net.URI;
import java.time.temporal.Temporal;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.LicenseUriParser;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;

/**
 * Interpreter for the multimedia extension, Interprets form {@link ExtendedRecord} to {@link
 * MultimediaRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/gbif/1.0/multimedia.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultimediaInterpreter {

  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();
  private static final LicenseUriParser LICENSE_URI_PARSER = LicenseUriParser.getInstance();

  private static final TargetHandler<Multimedia> HANDLER =
      ExtensionInterpretation.extension(Extension.MULTIMEDIA)
          .to(Multimedia::new)
          .map(DcTerm.references, MultimediaInterpreter::parseAndSetReferences)
          .mapOne(DcTerm.identifier, MultimediaInterpreter::parseAndSetIdentifier)
          .mapOne(DcTerm.created, MultimediaInterpreter::parseAndSetCreated)
          .map(DcTerm.license, MultimediaInterpreter::parseAndSetLicense)
          .map(DcTerm.rights, MultimediaInterpreter::parseAndSetLicense)
          .map(DcTerm.format, MultimediaInterpreter::parseAndSetFormatAndType)
          .map(DcTerm.title, Multimedia::setTitle)
          .map(DcTerm.description, Multimedia::setDescription)
          .map(DcTerm.contributor, Multimedia::setContributor)
          .map(DcTerm.publisher, Multimedia::setPublisher)
          .map(DcTerm.audience, Multimedia::setAudience)
          .map(DcTerm.creator, Multimedia::setCreator)
          .map(DcTerm.rightsHolder, Multimedia::setRightsHolder)
          .map(DcTerm.source, Multimedia::setSource)
          .map(DwcTerm.datasetID, Multimedia::setDatasetId)
          .postMap(MultimediaInterpreter::parseAndSetTypeFromReferences)
          .skipIf(MultimediaInterpreter::checkLinks);

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

  public static void interpretAssociatedMedia(ExtendedRecord er, MultimediaRecord mr) {

    Predicate<URI> prFn =
        uri ->
            mr.getMultimediaItems().stream()
                .anyMatch(
                    v ->
                        (!Strings.isNullOrEmpty(v.getIdentifier())
                                && uri.equals(URI.create(v.getIdentifier())))
                            || (!Strings.isNullOrEmpty(v.getReferences())
                                && uri.equals(URI.create(v.getReferences()))));

    extractOptValue(er, DwcTerm.associatedMedia)
        .ifPresent(
            v ->
                UrlParser.parseUriList(v)
                    .forEach(
                        uri -> {
                          if (uri == null) {
                            mr.getIssues().getIssueList().add(MULTIMEDIA_URI_INVALID.name());
                          } else if (!prFn.test(uri)) {
                            Multimedia multimedia = new Multimedia();
                            multimedia.setIdentifier(uri.toString());
                            parseAndSetFormatAndType(multimedia, null);
                            mr.getMultimediaItems().add(multimedia);
                          }
                        }));
  }

  /** Parser for "http://purl.org/dc/terms/references" term value */
  private static void parseAndSetReferences(Multimedia m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setReferences);
  }

  /** Parser for "http://purl.org/dc/terms/identifier" term value */
  private static String parseAndSetIdentifier(Multimedia m, String v) {
    URI uri = UrlParser.parse(v);
    Optional<URI> uriOpt = Optional.ofNullable(uri);
    if (uriOpt.isPresent()) {
      Optional<String> opt = uriOpt.map(URI::toString);
      if (opt.isPresent()) {
        opt.ifPresent(m::setIdentifier);
      } else {
        return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
      }
    } else {
      return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
    }
    return "";
  }

  /** Parser for "http://purl.org/dc/terms/type" term value */
  private static void parseAndSetType(Multimedia m, String v) {
    String v1 = Optional.ofNullable(v).orElse("");
    String format = Optional.ofNullable(m.getFormat()).orElse("");
    BiPredicate<String, MediaType> prFn =
        (s, mt) ->
            format.startsWith(s)
                || v1.toLowerCase().startsWith(s)
                || v1.toLowerCase().startsWith(mt.name().toLowerCase());

    if (prFn.test("video", MediaType.MovingImage)) {
      m.setType(MediaType.MovingImage.name());
    } else if (prFn.test("audio", MediaType.Sound)) {
      m.setType(MediaType.Sound.name());
    } else if (prFn.test("image", MediaType.StillImage)) {
      m.setType(MediaType.StillImage.name());
    }
  }

  /** Parser for "http://purl.org/dc/terms/created" term value */
  private static String parseAndSetCreated(Multimedia m, String v) {
    ParsedTemporal parsed = TemporalParser.parse(v);
    parsed.getFromOpt().map(Temporal::toString).ifPresent(m::setCreated);

    return parsed.getIssues().isEmpty() ? "" : MULTIMEDIA_DATE_INVALID.name();
  }

  /** Parser for "http://purl.org/dc/terms/format" term value */
  private static void parseAndSetFormatAndType(Multimedia m, String v) {
    String mimeType = MEDIA_PARSER.parseMimeType(v);
    if (Strings.isNullOrEmpty(mimeType) && !Strings.isNullOrEmpty(m.getIdentifier())) {
      try {
        mimeType = MEDIA_PARSER.parseMimeType(URI.create(m.getIdentifier()));
      } catch (IllegalArgumentException ex) {
        mimeType = null;
      }
    }
    if ("text/html".equalsIgnoreCase(mimeType) && m.getIdentifier() != null) {
      // make file URI the references link URL instead
      m.setReferences(m.getIdentifier());
      m.setIdentifier(null);
      mimeType = null;
    }

    m.setFormat(mimeType);

    parseAndSetType(m, m.getFormat());
  }

  /** Returns ENUM instead of url string */
  private static void parseAndSetLicense(Multimedia m, String v) {
    if (Objects.nonNull(v) && Objects.isNull(m.getLicense())) {
      ParseResult<URI> parsed = LICENSE_URI_PARSER.parse(v);
      m.setLicense(parsed.isSuccessful() ? parsed.getPayload().toString() : v);
    }
  }

  /** Skip whole record if both links are absent */
  private static Optional<String> checkLinks(Multimedia m) {
    if (m.getReferences() == null && m.getIdentifier() == null) {
      return Optional.of(MULTIMEDIA_URI_INVALID.name());
    }
    return Optional.empty();
  }

  /** Parses type in case if type is null, but maybe references contains type, like - *.jpg */
  private static void parseAndSetTypeFromReferences(Multimedia m) {
    if (m.getType() == null && (m.getReferences() != null || m.getIdentifier() != null)) {
      String value = m.getReferences() != null ? m.getReferences() : m.getIdentifier();
      parseAndSetFormatAndType(m, value);
    }
  }
}
