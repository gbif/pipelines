package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.model.collections.lookup.Match.Reason.DIFFERENT_OWNER;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.checkNullOrEmpty;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.pipelines.core.converters.GrscicollRecordConverter;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Match;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Status;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollInterpreter {

  public static BiConsumer<ExtendedRecord, GrscicollRecord> grscicollInterpreter(
      KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse> kvStore, MetadataRecord mdr) {
    return (er, gr) -> {
      if (kvStore == null || mdr == null) {
        throw new IllegalArgumentException("KvStore and MetadataRecord can't be null");
      }

      checkNullOrEmpty(er);

      if (!isSpecimenRecord(er)) {
        log.debug(
            "Skipped GrSciColl Lookup for record {} because it's not an specimen record",
            er.getId());
        return;
      }

      GrscicollLookupRequest lookupRequest =
          GrscicollLookupRequest.builder()
              .withInstitutionId(extractNullAwareValue(er, DwcTerm.institutionID))
              .withInstitutionCode(extractNullAwareValue(er, DwcTerm.institutionCode))
              .withOwnerInstitutionCode(extractNullAwareValue(er, DwcTerm.ownerInstitutionCode))
              .withCollectionId(extractNullAwareValue(er, DwcTerm.collectionID))
              .withCollectionCode(extractNullAwareValue(er, DwcTerm.collectionCode))
              .withDatasetKey(mdr.getDatasetKey())
              .withCountry(mdr.getDatasetPublishingCountry())
              .build();

      if (isEmptyRequest(lookupRequest)) {
        // skip the call
        log.debug(
            "Skipped GrSciColl Lookup for record {} due to missing collections fields", er.getId());
        return;
      }

      GrscicollLookupResponse lookupResponse = null;

      try {
        lookupResponse = kvStore.get(lookupRequest);
      } catch (Exception ex) {
        log.error("Error calling the GrSciColl lookup ws", ex);
      }

      if (isEmptyResponse(lookupResponse)) {
        // this shouldn't happen but we check it not to flag an issue in these cases
        log.warn("Empty GrSciColl lookup response for record {}", er.getId());
        return;
      }

      gr.setId(er.getId());

      // institution match
      Match institutionMatchResponse = lookupResponse.getInstitutionMatch();
      if (institutionMatchResponse.getMatchType() == GrscicollLookupResponse.MatchType.NONE) {
        addIssue(gr, getInstitutionMatchNoneIssue(institutionMatchResponse.getStatus()));

        // we skip the collections when there is no institution match
        return;
      }

      gr.setInstitutionMatch(GrscicollRecordConverter.convertMatch(institutionMatchResponse));

      if (institutionMatchResponse.getMatchType() == GrscicollLookupResponse.MatchType.FUZZY) {
        addIssue(gr, OccurrenceIssue.INSTITUTION_MATCH_FUZZY);
      }

      // https://github.com/gbif/registry/issues/496 we accept matches that have different owner,
      // but we flag them
      if (institutionMatchResponse.getReasons() != null
          && institutionMatchResponse.getReasons().contains(DIFFERENT_OWNER)) {
        addIssue(gr, OccurrenceIssue.DIFFERENT_OWNER_INSTITUTION);
      }

      // collection match
      Match collectionMatchResponse = lookupResponse.getCollectionMatch();
      if (collectionMatchResponse.getMatchType() == GrscicollLookupResponse.MatchType.NONE) {
        addIssue(gr, getCollectionMatchNoneIssue(collectionMatchResponse.getStatus()));
      } else {
        gr.setCollectionMatch(GrscicollRecordConverter.convertMatch(collectionMatchResponse));

        if (collectionMatchResponse.getMatchType() == GrscicollLookupResponse.MatchType.FUZZY) {
          addIssue(gr, OccurrenceIssue.COLLECTION_MATCH_FUZZY);
        }
      }
    };
  }

  private static boolean isSpecimenRecord(ExtendedRecord er) {

    Function<ParseResult<BasisOfRecord>, BasisOfRecord> fn =
        parseResult -> {
          if (parseResult.isSuccessful()) {
            return parseResult.getPayload();
          } else {
            return BasisOfRecord.OCCURRENCE;
          }
        };

    BasisOfRecord bor =
        VocabularyParser.basisOfRecordParser().map(er, fn).orElse(BasisOfRecord.OCCURRENCE);

    return bor == BasisOfRecord.PRESERVED_SPECIMEN
        || bor == BasisOfRecord.FOSSIL_SPECIMEN
        || bor == BasisOfRecord.LIVING_SPECIMEN
        || bor == BasisOfRecord.MATERIAL_SAMPLE
        || bor == BasisOfRecord.MATERIAL_CITATION;
  }

  @VisibleForTesting
  static OccurrenceIssue getInstitutionMatchNoneIssue(Status status) {
    if (status == Status.AMBIGUOUS || status == Status.AMBIGUOUS_EXPLICIT_MAPPINGS) {
      return OccurrenceIssue.AMBIGUOUS_INSTITUTION;
    }
    if (status == Status.AMBIGUOUS_OWNER) {
      return OccurrenceIssue.DIFFERENT_OWNER_INSTITUTION;
    }

    return OccurrenceIssue.INSTITUTION_MATCH_NONE;
  }

  @VisibleForTesting
  static OccurrenceIssue getCollectionMatchNoneIssue(Status status) {
    if (status == Status.AMBIGUOUS || status == Status.AMBIGUOUS_EXPLICIT_MAPPINGS) {
      return OccurrenceIssue.AMBIGUOUS_COLLECTION;
    }
    if (status == Status.AMBIGUOUS_INSTITUTION_MISMATCH) {
      return OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH;
    }

    return OccurrenceIssue.COLLECTION_MATCH_NONE;
  }

  private static boolean isEmptyRequest(GrscicollLookupRequest request) {
    return Strings.isNullOrEmpty(request.getInstitutionId())
        && Strings.isNullOrEmpty(request.getInstitutionCode())
        && Strings.isNullOrEmpty(request.getOwnerInstitutionCode())
        && Strings.isNullOrEmpty(request.getCollectionId())
        && Strings.isNullOrEmpty(request.getCollectionCode());
  }

  private static boolean isEmptyResponse(GrscicollLookupResponse response) {
    return response == null
        || (response.getInstitutionMatch() == null && response.getCollectionMatch() == null);
  }
}
