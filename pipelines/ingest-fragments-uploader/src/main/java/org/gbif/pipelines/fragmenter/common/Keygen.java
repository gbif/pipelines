package org.gbif.pipelines.fragmenter.common;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Keygen {

  private static final Long ERROR_KEY = -1L;

  public static Long getKey(HBaseLockingKeyService keygenService, ExtendedRecord er) {
    Set<String> set = new HashSet<>(2);

    // Adds occurrenceId
    String occurrenceId = extractValue(er, DwcTerm.occurrenceID);
    if (!Strings.isNullOrEmpty(occurrenceId)) {
      set.add(occurrenceId);
    }

    // Adds triplet
    String ic = extractValue(er, DwcTerm.institutionCode);
    String cc = extractValue(er, DwcTerm.collectionCode);
    String cn = extractValue(er, DwcTerm.catalogNumber);
    OccurrenceKeyBuilder.buildKey(ic, cc, cn).ifPresent(set::add);

    if (set.isEmpty()) {
      return ERROR_KEY;
    }

    // Finds or generate key
    KeyLookupResult keyResult = Optional.ofNullable(keygenService.findKey(set))
        .orElse(keygenService.generateKey(set));
    return Optional.ofNullable(keyResult).map(KeyLookupResult::getKey).orElse(ERROR_KEY);
  }

  private static String extractValue(ExtendedRecord er, Term term) {
    return er.getCoreTerms().get(term.qualifiedName());
  }

  public static Long getErrorKey() {
    return ERROR_KEY;
  }
}
