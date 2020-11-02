package org.gbif.pipelines.core.parsers.taxonomy;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.rest.client.species.NameUsageMatch;

/** Adapts a {@link NameUsageMatch} into a {@link TaxonRecord} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TaxonRecordConverter {

  /**
   * I modify the parameter instead of creating a new one and returning it because the lambda
   * parameters are final used in Interpreter.
   */
  public static void convert(NameUsageMatch nameUsageMatch, TaxonRecord taxonRecord) {
    Objects.requireNonNull(nameUsageMatch);
    convertInternal(nameUsageMatch, taxonRecord);
  }

  private static TaxonRecord convertInternal(NameUsageMatch source, TaxonRecord taxonRecord) {

    List<RankedName> classifications =
        source.getClassification().stream()
            .map(TaxonRecordConverter::convertRankedName)
            .collect(Collectors.toList());

    taxonRecord.setClassification(classifications);
    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(convertRankedName(source.getUsage()));
    // Usage is set as the accepted usage if the accepted usage is null
    taxonRecord.setAcceptedUsage(
        Optional.ofNullable(convertRankedName(source.getAcceptedUsage()))
            .orElse(taxonRecord.getUsage()));
    taxonRecord.setNomenclature(convertNomenclature(source.getNomenclature()));
    taxonRecord.setDiagnostics(convertDiagnostics(source.getDiagnostics()));

    return taxonRecord;
  }

  private static RankedName convertRankedName(org.gbif.api.v2.RankedName rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    return RankedName.newBuilder()
        .setKey(rankedNameApi.getKey())
        .setName(rankedNameApi.getName())
        .setRank(Rank.valueOf(rankedNameApi.getRank().name()))
        .build();
  }

  private static Nomenclature convertNomenclature(NameUsageMatch.Nomenclature nomenclatureApi) {
    if (nomenclatureApi == null) {
      return null;
    }

    return Nomenclature.newBuilder()
        .setId(nomenclatureApi.getId())
        .setSource(nomenclatureApi.getSource())
        .build();
  }

  private static Diagnostic convertDiagnostics(NameUsageMatch.Diagnostics diagnosticsApi) {
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
            .setLineage(diagnosticsApi.getLineage());

    // status. A bit of defensive programming...
    if (diagnosticsApi.getStatus() != null) {
      builder.setStatus(Status.valueOf(diagnosticsApi.getStatus().name()));
    }

    return builder.build();
  }
}
