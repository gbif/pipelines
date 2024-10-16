package org.gbif.pipelines.core.parsers.taxonomy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/** Adapts a {@link NameUsageMatchResponse} into a {@link TaxonRecord} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonRecordConverter {

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

    // nom code doesnt seem to be set...
    //    taxonRecord.setNomenclature(convertNomenclature(source.getNomenclature()));
    taxonRecord.setDiagnostics(convertDiagnostics(source.getDiagnostics()));

    // IUCN Red List Category
    Optional.ofNullable(source.getAdditionalStatus()).orElseGet(List::of).stream()
        .filter(status -> status.getGbifKey().equals("19491596-35ae-4a91-9a98-85cf505f1bd3"))
        .findFirst()
        .map(status -> status.getStatusCode())
        .ifPresent(taxonRecord::setIucnRedListCategoryCode);

    return taxonRecord;
  }

  private static RankedName convertUsage(NameUsageMatchResponse.Usage rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    return RankedName.newBuilder()
        .setKey(rankedNameApi.getKey())
        .setName(rankedNameApi.getName())
        .setRank(rankedNameApi.getRank())
        .setAuthorship(rankedNameApi.getAuthorship())
        .build();
  }

  private static RankedName convertRankedName(NameUsageMatchResponse.RankedName rankedNameApi) {
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
            .setAlternatives(alternatives)
            .setConfidence(diagnosticsApi.getConfidence())
            .setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()))
            .setNote(diagnosticsApi.getNote())
            .setLineage(List.of());

    // status. A bit of defensive programming...
    if (diagnosticsApi.getStatus() != null) {
      builder.setStatus(Status.valueOf(diagnosticsApi.getStatus()));
    }

    return builder.build();
  }
}
