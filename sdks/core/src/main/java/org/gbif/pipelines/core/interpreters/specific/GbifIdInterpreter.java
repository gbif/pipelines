package org.gbif.pipelines.core.interpreters.specific;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GbifIdInterpreter {

  public static final String GBIF_ID_INVALID = "GBIF_ID_INVALID";

  /** Copies GBIF id from ExtendedRecord id or generates/gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretGbifId(
      HBaseLockingKeyService keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn) {
    gbifIdFn = gbifIdFn == null ? interpretCopyGbifId() : gbifIdFn;
    return useExtendedRecordId
        ? gbifIdFn
        : interpretGbifId(keygenService, isTripletValid, isOccurrenceIdValid);
  }

  /** Generates or gets existing GBIF id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretGbifId(
      HBaseLockingKeyService keygenService, boolean isTripletValid, boolean isOccurrenceIdValid) {
    return (er, br) -> {
      if (keygenService == null) {
        return;
      }

      Optional<Long> gbifId =
          getOrGenerateGbifId(er, keygenService, isTripletValid, isOccurrenceIdValid, true);
      if (gbifId.isPresent()) {
        br.setGbifId(gbifId.get());
      } else {
        addIssue(br, GBIF_ID_INVALID);
      }
    };
  }

  /** Gets existing GBIF id */
  public static Function<ExtendedRecord, Optional<Long>> findGbifId(
      HBaseLockingKeyService keygenService, boolean isTripletValid, boolean isOccurrenceIdValid) {
    return er -> {
      if (keygenService == null) {
        log.error("keygenService is null");
        return Optional.empty();
      }

      return getOrGenerateGbifId(er, keygenService, isTripletValid, isOccurrenceIdValid, false);
    };
  }

  /** Copies GBIF id from ExtendedRecord id */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretCopyGbifId() {
    return (er, br) -> {
      if (StringUtils.isNumeric(er.getId())) {
        br.setGbifId(Long.parseLong(er.getId()));
      }
    };
  }

  /** Generates or gets existing GBIF id */
  private static Optional<Long> getOrGenerateGbifId(
      ExtendedRecord extendedRecord,
      HBaseLockingKeyService keygenService,
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean generateIfAbsent) {

    if (keygenService == null) {
      return Optional.empty();
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
      if (generateIfAbsent && keyResult == null) {
        keyResult = keygenService.generateKey(uniqueStrings);
      }
      return Optional.ofNullable(keyResult).map(KeyLookupResult::getKey);

    } catch (IllegalStateException ex) {
      log.warn(ex.getMessage());
      return Optional.empty();
    }
  }
}
