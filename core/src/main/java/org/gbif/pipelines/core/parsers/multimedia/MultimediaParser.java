package org.gbif.pipelines.core.parsers.multimedia;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.Terms;
import org.gbif.pipelines.core.parsers.InterpretationIssue;
import org.gbif.pipelines.core.parsers.ParsedField;
import org.gbif.pipelines.core.parsers.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.io.avro.MediaType;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser for the multimedia fields of a record.
 */
public class MultimediaParser {

  private static final Logger LOG = LoggerFactory.getLogger(MultimediaParser.class);

  // format prefixes
  private static final String IMAGE_FORMAT_PREFIX = "image";
  private static final String AUDIO_FORMAT_PREFIX = "audio";
  private static final String VIDEO_FORMAT_PREFIX = "video";

  private static final String HTML_TYPE = "text/html";
  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  // Order is important in case more than one extension is provided. The order will define the precedence.
  private static final Set<Extension> SUPPORTED_MEDIA_EXTENSIONS =
    ImmutableSet.of(Extension.MULTIMEDIA, Extension.AUDUBON, Extension.IMAGE);

  private MultimediaParser() {}

  /**
   * Parses the multimedia terms of a {@link ExtendedRecord}.
   *
   * @param extendedRecord record to be parsed
   *
   * @return {@link ParsedField} for a list of {@link ParsedMultimedia}.
   */
  public static ParsedField<List<ParsedMultimedia>> parseMultimedia(ExtendedRecord extendedRecord) {
    Objects.requireNonNull(extendedRecord);

    Map<URI, ParsedMultimedia> parsedMultimediaMap = new HashMap<>();
    List<InterpretationIssue> issues = new ArrayList<>();

    // check multimedia extensions first
    if (extendedRecord.getExtensions() != null) {
      // find the first multimedia extension supported and parse the records
      SUPPORTED_MEDIA_EXTENSIONS.stream()
        .filter(ext -> extendedRecord.getExtensions().containsKey(ext.getRowType()))
        .findFirst()
        .ifPresent(extension -> extendedRecord.getExtensions().get(extension.getRowType()).forEach(recordsMap -> {
          //For AUDUBON, we use accessURI over identifier
          Optional<TermWithValue> uriTermOpt = getTermWithValueOfFirst(recordsMap, AcTerm.accessURI, DcTerm.identifier);
          Optional<TermWithValue> linkTermOpt = getTermWithValueOfFirst(recordsMap,
                                                                        DcTerm.references,
                                                                        AcTerm.furtherInformationURL,
                                                                        AcTerm.attributionLinkURL);

          URI uri = UrlParser.parse(uriTermOpt.map(termWithValue -> termWithValue.value).orElse(null));
          URI link = UrlParser.parse(linkTermOpt.map(termWithValue -> termWithValue.value).orElse(null));

          // link or media uri must exist
          if (Objects.isNull(uri) && Objects.isNull(link)) {
            // find terms used to get the uri and the link and add issue
            List<Term> termsUsed = new ArrayList<>(2);
            uriTermOpt.ifPresent(termWithValue -> termsUsed.add(termWithValue.term));
            linkTermOpt.ifPresent(termWithValue -> termsUsed.add(termWithValue.term));
            issues.add(InterpretationIssue.of(IssueType.MULTIMEDIA_URI_INVALID, termsUsed));
            return;
          }

          // parse the atomic fields at once
          AtomicFields atomicFields = parseAtomicFields(recordsMap, uri, link);

          parsedMultimediaMap.computeIfAbsent(getPreferredIdentifier(atomicFields), identifier ->
            // create multimedia from extension
            ParsedMultimedia.newBuilder()
              .identifier(atomicFields.identifier)
              .references(atomicFields.references)
              .title(getTermValue(recordsMap, DcTerm.title))
              .description(getValueOfFirst(recordsMap, DcTerm.description, AcTerm.caption))
              .license(getValueOfFirst(recordsMap, DcTerm.license, DcTerm.rights))
              .publisher(getTermValue(recordsMap, DcTerm.publisher))
              .contributor(getTermValue(recordsMap, DcTerm.contributor))
              .source(getValueOfFirst(recordsMap, DcTerm.source, AcTerm.derivedFrom))
              .audience(getTermValue(recordsMap, DcTerm.audience))
              .rightsHolder(getTermValue(recordsMap, DcTerm.rightsHolder))
              .creator(getTermValue(recordsMap, DcTerm.creator))
              .format(atomicFields.format)
              .type(detectType(atomicFields.format))
              .created(parseCreatedDate(recordsMap, issues))
              .build());
        }));
    }

    // media via core term
    String associatedMedia = getTermValue(extendedRecord.getCoreTerms(), DwcTerm.associatedMedia);
    if (!Strings.isNullOrEmpty(associatedMedia)) {
      UrlParser.parseUriList(associatedMedia).forEach(uri -> {
        if (Objects.isNull(uri)) {
          issues.add(InterpretationIssue.of(IssueType.MULTIMEDIA_URI_INVALID, DwcTerm.associatedMedia));
          return;
        }

        // parse the atomic fields at once
        AtomicFields atomicFields = parseAtomicFields(extendedRecord.getCoreTerms(), uri, null);

        parsedMultimediaMap.computeIfAbsent(getPreferredIdentifier(atomicFields), identifier ->
          // create multimedia from core
          ParsedMultimedia.newBuilder()
            .identifier(atomicFields.identifier)
            .references(atomicFields.references)
            .format(atomicFields.format)
            .type(detectType(atomicFields.format))
            .build());
      });
    }

    return parsedMultimediaMap.size() > 0 ? ParsedField.success(Lists.newArrayList(parsedMultimediaMap.values()),
                                                                issues) : ParsedField.fail(issues);
  }

  private static AtomicFields parseAtomicFields(Map<String, String> recordsMap, URI identifier, URI link) {
    // get format
    String format = Optional.ofNullable(MEDIA_PARSER.parseMimeType(getTermValue(recordsMap, DcTerm.format)))
      .filter(value -> !value.isEmpty())
      .orElseGet(() -> MEDIA_PARSER.parseMimeType(identifier));

    AtomicFields atomicFields = new AtomicFields();
    if (HTML_TYPE.equalsIgnoreCase(format) && Objects.nonNull(identifier)) {
      atomicFields.references = identifier;
      return atomicFields;
    }

    atomicFields.references = link;
    atomicFields.identifier = identifier;
    atomicFields.format = format;

    return atomicFields;
  }

  private static Temporal parseCreatedDate(Map<String, String> recordsMap, List<InterpretationIssue> issues) {
    ParsedTemporalDates temporalDate = TemporalParser.parse(getTermValue(recordsMap, DcTerm.created));

    if (Objects.nonNull(temporalDate.getIssueList())) {
      temporalDate.getIssueList()
        .forEach(issueType -> issues.add(InterpretationIssue.of(issueType, DcTerm.created)));
    }

    return temporalDate.getFrom().orElse(null);
  }

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

    LOG.debug("Unsupported media format {}", format);
    return null;
  }

  private static String getTermValue(Map<String, String> recordsMap, Term term) {
    return recordsMap.get(term.qualifiedName());
  }

  private static String getValueOfFirst(Map<String, String> record, Term... terms) {
    return Arrays.stream(terms)
      .filter(term -> record.containsKey(term.qualifiedName()))
      .map(term -> cleanTerm(getTermValue(record, term)))
      .filter(Objects::nonNull)
      .findFirst()
      .orElse(null);
  }

  private static Optional<TermWithValue> getTermWithValueOfFirst(Map<String, String> record, Term... terms) {
    return Arrays.stream(terms)
      .filter(term -> record.containsKey(term.qualifiedName()))
      .map(term -> new TermWithValue(term, cleanTerm(getTermValue(record, term))))
      .filter(termWithValue -> Objects.nonNull(termWithValue.value))
      .findFirst();
  }

  private static String cleanTerm(String str) {
    return Terms.isTermValueBlank(str) ? null : Strings.emptyToNull(str.trim());
  }

  private static URI getPreferredIdentifier(AtomicFields atomicFields) {
    return atomicFields.identifier != null ? atomicFields.identifier : atomicFields.references;
  }

  /**
   * {@link Term} with its value.
   */
  private static class TermWithValue {

    final Term term;
    final String value;

    TermWithValue(Term term, String value) {
      this.term = term;
      this.value = value;
    }
  }

  /**
   * Fields that have to be parsed at once.
   */
  private static class AtomicFields {

    String format;
    URI identifier;
    URI references;
  }
}