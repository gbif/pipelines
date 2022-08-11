package org.gbif.pipelines.core.interpreters.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.SimpleOccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifIdInterpreter {

  /** Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, GbifIdRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      boolean generateIdIfAbsent,
      BiConsumer<ExtendedRecord, GbifIdRecord> gbifIdFn) {
    gbifIdFn = gbifIdFn == null ? interpretCopyGbifId() : gbifIdFn;
    return useExtendedRecordId
        ? gbifIdFn
        : interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent);
  }

  /** Generates or gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, GbifIdRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent) {
    return (er, gr) -> {
      SimpleOccurrenceRecord occRecords = SimpleOccurrenceRecord.create();

      // Adds occurrenceId
      if (isOccurrenceIdValid) {
        String occurrenceId = extractValue(er, DwcTerm.occurrenceID);
        if (!Strings.isNullOrEmpty(occurrenceId)) {
          occRecords.setOccurrenceId(occurrenceId);
          gr.setOccurrenceId(occurrenceId);
        }
      }

      // Adds triplet
      if (isTripletValid) {
        String ic = extractValue(er, DwcTerm.institutionCode);
        String cc = extractValue(er, DwcTerm.collectionCode);
        String cn = extractValue(er, DwcTerm.catalogNumber);
        OccurrenceKeyBuilder.buildKey(ic, cc, cn)
            .ifPresent(
                tr -> {
                  occRecords.setTriplet(tr);
                  gr.setTriplet(tr);
                });
      }

      Optional<Long> gbifId =
          Keygen.getKey(
              keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent, occRecords);
      if (gbifId.isPresent() && !Keygen.getErrorKey().equals(gbifId.get())) {
        gr.setGbifId(gbifId.get());
      } else if (!generateIdIfAbsent) {
        addIssue(gr, GBIF_ID_ABSENT);
      } else {
        addIssue(gr, GBIF_ID_INVALID);
      }
    };
  }

  /** Generates or gets existing GBIF id */
  public static Consumer<GbifIdRecord> interpretAbsentGbifId(
      HBaseLockingKey keygenService, boolean isTripletValid, boolean isOccurrenceIdValid) {
    return gr -> {
      SimpleOccurrenceRecord occRecords = SimpleOccurrenceRecord.create();

      // Adds occurrenceId
      if (isOccurrenceIdValid && !Strings.isNullOrEmpty(gr.getOccurrenceId())) {
        occRecords.setOccurrenceId(gr.getOccurrenceId());
      }

      // Adds triplet, if isTripletValid and isOccurrenceIdValid is false, or occurrenceId is null
      if (isTripletValid && !Strings.isNullOrEmpty(gr.getTriplet())) {
        occRecords.setTriplet(gr.getTriplet());
      }

      Optional<Long> gbifId =
          Keygen.getKey(keygenService, isTripletValid, isOccurrenceIdValid, true, occRecords);

      if (gbifId.isPresent() && !Keygen.getErrorKey().equals(gbifId.get())) {
        gr.setGbifId(gbifId.get());
        gr.getIssues().setIssueList(Collections.emptyList());
      } else {
        gr.getIssues().setIssueList(Collections.singletonList(GBIF_ID_INVALID));
      }
    };
  }

  /** Copies GBIF id from ExtendedRecord id */
  public static BiConsumer<ExtendedRecord, GbifIdRecord> interpretCopyGbifId() {
    return (er, gr) -> {
      if (StringUtils.isNumeric(er.getId())) {
        gr.setGbifId(Long.parseLong(er.getId()));
      }
    };
  }
}
