package org.gbif.pipelines.parsers.parsers.multimedia;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.Terms;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

/** Parser for the multimedia fields of a record. */
public class MultimediaParser {

  // format prefixes
  private static final String IMAGE_FORMAT_PREFIX = "image";
  private static final String AUDIO_FORMAT_PREFIX = "audio";
  private static final String VIDEO_FORMAT_PREFIX = "video";

  private static final String HTML_TYPE = "text/html";
  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  // Order is important in case more than one extension is provided. The order will define the
  // precedence.
  private static final EnumSet<Extension> SUPPORTED_MEDIA_EXTENSIONS =
          EnumSet.of(Extension.MULTIMEDIA, Extension.AUDUBON, Extension.IMAGE);

  private MultimediaParser() {}

  /**
   * Parses the multimedia terms of a {@link ExtendedRecord}.
   *
   * @param extendedRecord record to be parsed
   * @return {@link ParsedField} for a list of {@link ParsedMultimedia}.
   */
  public static ParsedField<List<ParsedMultimedia>> parseMultimedia(ExtendedRecord extendedRecord) {
    Objects.requireNonNull(extendedRecord);

    Map<URI, ParsedMultimedia> parsedMultimediaMap = new HashMap<>();
    List<String> issues = new ArrayList<>();

    parseExtensions(extendedRecord, parsedMultimediaMap, issues);
    parseCoreTerm(extendedRecord, parsedMultimediaMap, issues);

    return parsedMultimediaMap.size() > 0
        ? ParsedField.success(Lists.newArrayList(parsedMultimediaMap.values()), issues)
        : ParsedField.fail(issues);
  }

  /** */
  private static void parseCoreTerm(
      ExtendedRecord er, Map<URI, ParsedMultimedia> multimediaMap, List<String> issues) {
    String associatedMedia = getTermValue(er.getCoreTerms(), DwcTerm.associatedMedia);
    if (!Strings.isNullOrEmpty(associatedMedia)) {
      UrlParser.parseUriList(associatedMedia)
          .forEach(uri -> parseCoreTermMedia(er, multimediaMap, issues, uri));
    }
  }

  /** */
  private static void parseCoreTermMedia(
      ExtendedRecord er, Map<URI, ParsedMultimedia> multimediaMap, List<String> issues, URI uri) {

    if (uri == null) {
      issues.add(MULTIMEDIA_URI_INVALID.name());
      return;
    }

    // parse the fields at once
    Fields fields = parseFields(er.getCoreTerms(), uri, null);

    // create multimedia from core
    Function<URI, ParsedMultimedia> fn =
        u ->
            ParsedMultimedia.newBuilder()
                .identifier(fields.identifier)
                .references(fields.references)
                .format(fields.format)
                .type(detectType(fields.format))
                .build();

    multimediaMap.computeIfAbsent(getPreferredIdentifier(fields), fn);
  }

  /** */
  private static void parseExtensions(
      ExtendedRecord er, Map<URI, ParsedMultimedia> multimediaMap, List<String> issues) {
    // check multimedia extensions first
    if (er.getExtensions() != null) {

      Consumer<Extension> consumer =
          extension ->
              er.getExtensions()
                  .get(extension.getRowType())
                  .forEach(recordsMap -> parseExtensionsMedia(recordsMap, multimediaMap, issues));

      // find the first multimedia extension supported and parse the records
      SUPPORTED_MEDIA_EXTENSIONS
          .stream()
          .filter(ext -> er.getExtensions().containsKey(ext.getRowType()))
          .findFirst()
          .ifPresent(consumer);
    }
  }

  /** */
  private static void parseExtensionsMedia(
      Map<String, String> recordsMap,
      Map<URI, ParsedMultimedia> multimediaMap,
      List<String> issues) {

    // For AUDUBON, we use accessURI over identifier
    URI uri = getFirstUri(recordsMap, AcTerm.accessURI, DcTerm.identifier);
    URI link =
        getFirstUri(
            recordsMap, DcTerm.references, AcTerm.furtherInformationURL, AcTerm.attributionLinkURL);

    // link or media uri must exist
    if (uri == null && link == null) {
      issues.add(MULTIMEDIA_URI_INVALID.name());
      return;
    }

    // parse the fields at once
    Fields fields = parseFields(recordsMap, uri, link);

    Function<URI, ParsedMultimedia> fn =
        u ->
            ParsedMultimedia.newBuilder()
                .identifier(fields.identifier)
                .references(fields.references)
                .title(getTermValue(recordsMap, DcTerm.title))
                .description(getValueOfFirst(recordsMap, DcTerm.description, AcTerm.caption))
                .license(getValueOfFirst(recordsMap, DcTerm.license, DcTerm.rights))
                .publisher(getTermValue(recordsMap, DcTerm.publisher))
                .contributor(getTermValue(recordsMap, DcTerm.contributor))
                .source(getValueOfFirst(recordsMap, DcTerm.source, AcTerm.derivedFrom))
                .audience(getTermValue(recordsMap, DcTerm.audience))
                .rightsHolder(getTermValue(recordsMap, DcTerm.rightsHolder))
                .creator(getTermValue(recordsMap, DcTerm.creator))
                .format(fields.format)
                .type(detectType(fields.format))
                .created(parseCreatedDate(recordsMap, issues))
                .build();

    // create multimedia from extension
    multimediaMap.computeIfAbsent(getPreferredIdentifier(fields), fn);
  }

  /** */
  private static Fields parseFields(Map<String, String> recordsMap, URI identifier, URI link) {
    // get format
    String mimeType = MEDIA_PARSER.parseMimeType(getTermValue(recordsMap, DcTerm.format));
    String format =
        Optional.ofNullable(mimeType)
            .filter(value -> !value.isEmpty())
            .orElseGet(() -> MEDIA_PARSER.parseMimeType(identifier));

    Fields fields = new Fields();
    if (HTML_TYPE.equalsIgnoreCase(format) && identifier != null) {
      fields.references = identifier;
      return fields;
    }

    fields.references = link;
    fields.identifier = identifier;
    fields.format = format;

    return fields;
  }

  /** */
  private static Temporal parseCreatedDate(Map<String, String> recordsMap, List<String> issues) {
    ParsedTemporal temporalDate =
        TemporalParser.parse(getTermValue(recordsMap, DcTerm.created));

    if (temporalDate.getIssueList() != null) {
      issues.addAll(temporalDate.getIssueList());
    }

    return temporalDate.getFrom().orElse(null);
  }

  /** */
  private static MediaType detectType(String format) {
    if (!Strings.isNullOrEmpty(format)) {
      if (format.toLowerCase().startsWith(IMAGE_FORMAT_PREFIX)) {
        return MediaType.StillImage;
      }
      if (format.toLowerCase().startsWith(AUDIO_FORMAT_PREFIX)) {
        return MediaType.Sound;
      }
      if (format.toLowerCase().startsWith(VIDEO_FORMAT_PREFIX)) {
        return MediaType.MovingImage;
      }
    }

    return null;
  }

  /** */
  private static String getTermValue(Map<String, String> recordsMap, Term term) {
    return recordsMap.get(term.qualifiedName());
  }

  /** */
  private static String getValueOfFirst(Map<String, String> record, Term... terms) {
    return Arrays.stream(terms)
        .filter(term -> record.containsKey(term.qualifiedName()))
        .map(term -> cleanTerm(getTermValue(record, term)))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  /** */
  private static URI getFirstUri(Map<String, String> record, Term... terms) {
    String result =
        Arrays.stream(terms)
            .filter(term -> record.containsKey(term.qualifiedName()))
            .map(term -> new TermWithValue(term, cleanTerm(getTermValue(record, term))).value)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    return UrlParser.parse(result);
  }

  /** */
  private static String cleanTerm(String str) {
    return Terms.isTermValueBlank(str) ? null : Strings.emptyToNull(str.trim());
  }

  /** */
  private static URI getPreferredIdentifier(Fields fields) {
    return fields.identifier != null ? fields.identifier : fields.references;
  }

  /** {@link Term} with its value. */
  private static class TermWithValue {

    final Term term;
    final String value;

    TermWithValue(Term term, String value) {
      this.term = term;
      this.value = value;
    }
  }

  /** Fields that have to be parsed at once. */
  private static class Fields {

    String format;
    URI identifier;
    URI references;
  }
}
