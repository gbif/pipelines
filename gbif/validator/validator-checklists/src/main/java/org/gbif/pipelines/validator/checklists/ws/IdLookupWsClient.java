package org.gbif.pipelines.validator.checklists.ws;

import jakarta.annotation.Nullable;
import java.util.Optional;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.nub.lookup.straight.LookupUsage;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/** Ws Client for the species/name lookup service. */
@RequestMapping(value = "/species/lookup", produces = MediaType.APPLICATION_JSON_VALUE)
public interface IdLookupWsClient {

  /** Proxy method to the remote call based on string parameters. */
  default LookupUsage match(
      String canonicalName,
      @Nullable String authorship,
      @Nullable String year,
      Rank rank,
      TaxonomicStatus status,
      Kingdom kingdom) {
    return match(
        canonicalName,
        authorship,
        year,
        Optional.ofNullable(rank).map(Enum::name).orElse(null),
        Optional.ofNullable(status).map(Enum::name).orElse(null),
        Optional.ofNullable(kingdom).map(Enum::name).orElse(null));
  }

  /** Remote call. */
  @RequestMapping
  LookupUsage match(
      @RequestParam(value = "name") String canonicalName,
      @RequestParam(value = "authorship", required = false) String authorship,
      @RequestParam(value = "year", required = false) String year,
      @RequestParam(value = "rank", required = false) String rank,
      @RequestParam(value = "status", required = false) String status,
      @RequestParam(value = "kingdom", required = false) String kingdom);
}
