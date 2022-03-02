package org.gbif.pipelines.core.interpreters.specific;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifIdInterpreter {

  public static final String GBIF_ID_INVALID = "GBIF_ID_INVALID";

  /** Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, GbifIdRecord> interpretGbifId(
      HBaseLockingKeyService keygenService,
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
      HBaseLockingKeyService keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent) {
    return (er, gr) -> {
      Optional<Long> gbifId =
          getOrGenerateGbifId(
              er, keygenService, isTripletValid, isOccurrenceIdValid, generateIdIfAbsent);
      if (gbifId.isPresent()) {
        gr.setGbifId(gbifId.get());
      } else {
        addIssue(gr, GBIF_ID_INVALID);
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

  /** Generates or gets existing GBIF id */
  private static Optional<Long> getOrGenerateGbifId(
      ExtendedRecord extendedRecord,
      HBaseLockingKeyService keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIdIfAbsent) {

    if (keygenService == null) {
      throw new PipelinesException("keygenService can't be null!");
    }

    Set<String> uniqueStrings = new HashSet<>(2);

    // Adds occurrenceId
    if (isOccurrenceIdValid) {
      String occurrenceId = extractValue(extendedRecord, DwcTerm.occurrenceID);
      if (!Strings.isNullOrEmpty(occurrenceId)) {
        uniqueStrings.add(occurrenceId);
      }
    }

    // Adds triplet
    if (isTripletValid) {
      String ic = extractValue(extendedRecord, DwcTerm.institutionCode);
      String cc = extractValue(extendedRecord, DwcTerm.collectionCode);
      String cn = extractValue(extendedRecord, DwcTerm.catalogNumber);
      OccurrenceKeyBuilder.buildKey(ic, cc, cn).ifPresent(uniqueStrings::add);
    }

    if (uniqueStrings.isEmpty()) {
      return Optional.empty();
    }

    try {
      // Finds or generates key
      KeyLookupResult keyResult = keygenService.findKey(uniqueStrings);
      if (generateIdIfAbsent && keyResult == null) {
        keyResult = keygenService.generateKey(uniqueStrings);
      }
      return Optional.ofNullable(keyResult).map(KeyLookupResult::getKey);

    } catch (IllegalStateException ex) {
      log.warn(ex.getMessage());
      return Optional.empty();
    }
  }
}
