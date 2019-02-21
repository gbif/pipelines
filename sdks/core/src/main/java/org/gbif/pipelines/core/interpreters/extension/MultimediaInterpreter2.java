package org.gbif.pipelines.core.interpreters.extension;

import java.net.URI;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
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
import org.gbif.pipelines.parsers.utils.ModelUtils;

import com.google.common.base.Strings;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

public class MultimediaInterpreter2 {

  private static final TargetHandler<Multimedia> HANDLER =
      ExtensionInterpretation.extenstion(Extension.MULTIMEDIA)
          .to(Multimedia::new)
          .map(DcTerm.format, Multimedia::setFormat)
          .map(DcTerm.identifier, Multimedia::setIdentifier)
          .map(DcTerm.title, Multimedia::setTitle)
          .map(DcTerm.description, Multimedia::setDescription)
          .map(DcTerm.contributor, Multimedia::setContributor)
          .map(DcTerm.publisher, Multimedia::setPublisher)
          .map(DcTerm.audience, Multimedia::setSource)
          .map(DcTerm.license, Multimedia::setLicense)
          .map(DcTerm.rightsHolder, Multimedia::setRightsHolder)
          .map(DwcTerm.datasetID, Multimedia::setDatasetId)
          .map(DcTerm.type, MultimediaInterpreter2::parseAndSetType)
          .mapIssue(DcTerm.references, MultimediaInterpreter2::parseAndsetReferences)
          .mapIssues(DcTerm.created, MultimediaInterpreter2::parseAndSetCreated);

  //                  .setFormat(fields.format)
  //    Optional.ofNullable(fields.identifier).map(URI::toString).ifPresent(b::setIdentifier);

  private MultimediaInterpreter2() {}

  public static void interpret(ExtendedRecord er, MultimediaRecord mr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(mr);

    Result<Multimedia> result = HANDLER.convert(er);

    mr.setMultimediaItems(result.getList());
    ModelUtils.addIssue(mr, result.getIssues());
  }

  /**
   *
   */
  private static String parseAndsetReferences(Multimedia m, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(m::setReferences);
    return uri == null ? MULTIMEDIA_URI_INVALID.name() : null;
  }

  /**
   *
   */
  private static void parseAndSetType(Multimedia m, String v) {
    if (!Strings.isNullOrEmpty(v)) {
      if (v.toLowerCase().startsWith("image")) {
        m.setType(MediaType.StillImage);
      } else if (v.toLowerCase().startsWith("audio")) {
        m.setType(MediaType.Sound);
      } else if (v.toLowerCase().startsWith("video")) {
        m.setType(MediaType.MovingImage);
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
}
