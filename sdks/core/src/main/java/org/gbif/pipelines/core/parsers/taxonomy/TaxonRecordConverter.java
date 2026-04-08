package org.gbif.pipelines.core.parsers.taxonomy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.Constants;
import org.gbif.pipelines.io.avro.*;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Adapts a {@link NameUsageMatchResponse} into a {@link TaxonRecord} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonRecordConverter {

  public static final String IUCN_REDLIST_GBIF_KEY = Constants.IUCN_DATASET_KEY.toString();

  /**
   * I modify the parameter instead of creating a new one and returning it because the lambda
   * parameters are final used in Interpreter.
   */
  public static void convert(NameUsageMatchResponse nameUsageMatch, TaxonRecord taxonRecord) {
    Objects.requireNonNull(nameUsageMatch);
    convertInternal(nameUsageMatch, taxonRecord);
  }

  private static TaxonRecord convertInternal(
      NameUsageMatchResponse source, TaxonRecord taxonRecord) {

    List<RankedName> classifications =
        source.getClassification().stream()
            .map(TaxonRecordConverter::convertRankedName)
            .collect(Collectors.toList());

    taxonRecord.setClassification(classifications);
    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(convertUsage(source.getUsage()));

    // Usage is set as the accepted usage if the accepted usage is null
    taxonRecord.setAcceptedUsage(
        Optional.ofNullable(convertUsage(source.getAcceptedUsage()))
            .orElse(taxonRecord.getUsage()));

    taxonRecord.setDiagnostics(convertDiagnostics(source.getDiagnostics()));

    // IUCN Red List Category
    Optional.ofNullable(source.getAdditionalStatus()).orElseGet(List::of).stream()
        .filter(status -> status.getDatasetKey().equals(IUCN_REDLIST_GBIF_KEY))
        .findFirst()
        .map(status -> status.getStatusCode())
        .ifPresent(taxonRecord::setIucnRedListCategoryCode);

    return taxonRecord;
  }

  private static RankedNameWithAuthorship convertUsage(NameUsageMatchResponse.Usage rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    return RankedNameWithAuthorship.newBuilder()
        .setKey(rankedNameApi.getKey())
        .setName(rankedNameApi.getName())
        .setRank(rankedNameApi.getRank())
        .setAuthorship(rankedNameApi.getAuthorship())
        .setCode(rankedNameApi.getCode())
        .setInfragenericEpithet(rankedNameApi.getInfragenericEpithet())
        .setInfraspecificEpithet(rankedNameApi.getInfraspecificEpithet())
        .setSpecificEpithet(rankedNameApi.getSpecificEpithet())
        .setFormattedName(rankedNameApi.getFormattedName())
        .setStatus(rankedNameApi.getStatus())
        .setGenericName(rankedNameApi.getGenericName())
        .build();
  }

  public static RankedName convertRankedName(NameUsageMatchResponse.RankedName rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    return RankedName.newBuilder()
        .setKey(rankedNameApi.getKey())
        .setName(rankedNameApi.getName())
        .setRank(rankedNameApi.getRank())
        .build();
  }

  private static Diagnostic convertDiagnostics(NameUsageMatchResponse.Diagnostics diagnosticsApi) {
    if (diagnosticsApi == null) {
      return null;
    }

    // alternatives
    List<TaxonRecord> alternatives =
        diagnosticsApi.getAlternatives().stream()
            .map(match -> convertInternal(match, TaxonRecord.newBuilder().build()))
            .collect(Collectors.toList());

    Diagnostic.Builder builder =
        Diagnostic.newBuilder()
            .setConfidence(diagnosticsApi.getConfidence())
            .setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()))
            .setNote(diagnosticsApi.getNote())
            .setLineage(List.of());

    return builder.build();
  }
}
