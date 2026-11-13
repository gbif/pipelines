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
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.Keygen;
import org.gbif.pipelines.keygen.SimpleOccurrenceRecord;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
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
    return interpretGbifId(
        keygenService,
        isTripletValid,
        isOccurrenceIdValid,
        useExtendedRecordId,
        generateIdIfAbsent,
        IdPriority.DEFAULT,
        gbifIdFn);
  }

  /**
   * Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id.
   *
   * @param idPriority the identifier forced to be used and looked up first, see {@link
   *     #interpretGbifId(HBaseLockingKey, boolean, boolean, boolean, IdPriority)}
   */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      boolean generateIdIfAbsent,
      IdPriority idPriority,
      BiConsumer<ExtendedRecord, IdentifierRecord> gbifIdFn) {
    gbifIdFn = gbifIdFn == null ? interpretCopyGbifId() : gbifIdFn;
    return useExtendedRecordId
        ? gbifIdFn
        : interpretGbifId(
            keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent, idPriority);
  }

  /** Generates or gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent) {
    return interpretGbifId(
        keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent, IdPriority.DEFAULT);
  }

  /**
   * Generates or gets existing GBIF id.
   *
   * <p>By default an existing occurrenceID takes priority over the triplet, and each is only used
   * if its flag is valid. {@link IdPriority#OCCURRENCE_ID} uses the occurrenceID regardless of its
   * flag. {@link IdPriority#TRIPLET} uses the triplet regardless of its flag and looks it up first,
   * using its GBIF id if it exists and leaving the occurrenceID lookup row untouched. If the
   * triplet is not found, the default lookup is used.
   */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretGbifId(
      HBaseLockingKey keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent,
      IdPriority idPriority) {
    IdPriority priority = IdPriority.orDefault(idPriority);
    boolean useTriplet = priority.useTriplet(isTripletValid);
    boolean useOccurrenceId = priority.useOccurrenceId(isOccurrenceIdValid);
    return (er, ir) -> {
      SimpleOccurrenceRecord occRecords = SimpleOccurrenceRecord.create();

      // Adds occurrenceId
      if (useOccurrenceId) {
        String occurrenceId = extractValue(er, DwcTerm.occurrenceID);
        if (!Strings.isNullOrEmpty(occurrenceId)) {
          occRecords.setOccurrenceId(occurrenceId);
          ir.setUniqueKey(occurrenceId);
        }
      }

      // Adds triplet
      if (useTriplet) {
        String ic = extractValue(er, DwcTerm.institutionCode);
        String cc = extractValue(er, DwcTerm.collectionCode);
        String cn = extractValue(er, DwcTerm.catalogNumber);
        OccurrenceKeyBuilder.buildKey(ic, cc, cn)
            .ifPresent(
                tr -> {
                  occRecords.setTriplet(tr);
                  ir.setAssociatedKey(tr);
                });
      }

      Optional<Long> gbifId = Optional.empty();
      if (priority.isTripletFirst()) {
        gbifId = findTripletKey(keygenService, ir.getAssociatedKey());
      }
      if (gbifId.isEmpty()) {
        gbifId =
            Keygen.getKey(
                keygenService, useTriplet, useOccurrenceId, generateIdIfAbsent, occRecords);
      }
      if (gbifId.isPresent() && !Keygen.getErrorKey().equals(gbifId.get())) {
        ir.setInternalId(gbifId.get().toString());
      } else if (!generateIdIfAbsent) {
        addIssue(ir, GBIF_ID_ABSENT);
      } else {
        addIssue(ir, GBIF_ID_INVALID);
      }
    };
  }

  /**
   * Looks up the GBIF id of a triplet on its own, so that no other lookup rows are filled in.
   *
   * @return the GBIF id, the error key if the lookup failed, or empty if there is no triplet or it
   *     is not found
   */
  private static Optional<Long> findTripletKey(HBaseLockingKey keygenService, String triplet) {
    if (Strings.isNullOrEmpty(triplet)) {
      return Optional.empty();
    }
    try {
      return keygenService.findKey(Collections.singleton(triplet)).map(KeyLookupResult::getKey);
    } catch (RuntimeException ex) {
      log.error(ex.getMessage(), ex);
      return Optional.of(Keygen.getErrorKey());
    }
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

  private static final java.util.regex.Pattern UUID_PATTERN =
      java.util.regex.Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          java.util.regex.Pattern.CASE_INSENSITIVE);

  /** Copies GBIF id from ExtendedRecord id */
  public static BiConsumer<ExtendedRecord, IdentifierRecord> interpretCopyGbifId() {
    return (er, gr) -> {
      if (StringUtils.isNumeric(er.getId())) {
        gr.setInternalId(er.getId());
      }
      if (UUID_PATTERN.matcher(er.getId()).matches()) {
        gr.setInternalId(er.getId());
      }
    };
  }
}
