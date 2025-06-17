package org.gbif.pipelines.core.interpreters.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;

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
import org.gbif.pipelines.core.interpreters.model.ExtendedRecord;
import org.gbif.pipelines.core.interpreters.model.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.SimpleOccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifIdInterpreter {

  /** Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      boolean generateIdIfAbsent,
      BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn) {
    gbifIdFn = gbifIdFn == null ? interpretCopyGbifId() : gbifIdFn;
    return useExtendedRecordId
        ? gbifIdFn
        : interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent);
  }

  /** Generates or gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent) {
    return (er, ir) -> {
      SimpleOccurrenceRecord occRecords = SimpleOccurrenceRecord.create();

      // Adds occurrenceId
      if (isOccurrenceIdValid) {
        String occurrenceId = er.extractValue(DwcTerm.occurrenceID);
        if (!Strings.isNullOrEmpty(occurrenceId)) {
          occRecords.setOccurrenceId(occurrenceId);
          ir.setUniqueKey(occurrenceId);
        }
      }

      // Adds triplet
      if (isTripletValid) {
        String ic = er.extractValue(DwcTerm.institutionCode);
        String cc = er.extractValue(DwcTerm.collectionCode);
        String cn = er.extractValue(DwcTerm.catalogNumber);
        OccurrenceKeyBuilder.buildKey(ic, cc, cn)
            .ifPresent(
                tr -> {
                  occRecords.setTriplet(tr);
                  ir.setAssociatedKey(tr);
                });
      }

      Optional<Long> gbifId =
          Keygen.getKey(
              keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent, occRecords);
      if (gbifId.isPresent() && !Keygen.getErrorKey().equals(gbifId.get())) {
        ir.setInternalId(gbifId.get().toString());
      } else if (!generateIdIfAbsent) {
        ir.addIssue(GBIF_ID_ABSENT);
      } else {
        ir.addIssue(GBIF_ID_INVALID);
      }
    };
  }

  /** Generates or gets existing GBIF id */
  public static Consumer<IdentifierRecord> interpretAbsentGbifId(
      HBaseLockingKey keygenService, boolean isTripletValid, boolean isOccurrenceIdValid) {
    return ir -> {
      SimpleOccurrenceRecord occRecords = SimpleOccurrenceRecord.create();

      // Adds occurrenceId
      if (isOccurrenceIdValid && !Strings.isNullOrEmpty(ir.getUniqueKey())) {
        occRecords.setOccurrenceId(ir.getUniqueKey());
      }

      // Adds triplet, if isTripletValid and isOccurrenceIdValid is false, or occurrenceId is null
      if (isTripletValid && !Strings.isNullOrEmpty(ir.getAssociatedKey())) {
        occRecords.setTriplet(ir.getAssociatedKey());
      }

      Optional<Long> gbifId =
          Keygen.getKey(keygenService, isTripletValid, isOccurrenceIdValid, true, occRecords);

      if (gbifId.isPresent() && !Keygen.getErrorKey().equals(gbifId.get())) {
        ir.setInternalId(gbifId.get().toString());
        ir.getIssues().setIssueList(Collections.emptyList());
      } else {
        ir.getIssues().setIssueList(Collections.singletonList(GBIF_ID_INVALID));
      }
    };
  }

  /** Copies GBIF id from ExtendedRecord id */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretCopyGbifId() {
    return (er, gr) -> {
      if (StringUtils.isNumeric(er.getId())) {
        gr.setInternalId(er.getId());
      }
    };
  }
}
