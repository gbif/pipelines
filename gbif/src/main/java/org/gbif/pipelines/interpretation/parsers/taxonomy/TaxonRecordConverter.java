package org.gbif.pipelines.interpretation.parsers.taxonomy;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.interpretation.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.Diagnostics;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Adapts a {@link NameUsageMatch2} into a {@link TaxonRecord}
 */
public class TaxonRecordConverter {

  private TaxonRecordConverter() {}

  /**
   * I modify the parameter instead of creating a new one and returning it because the lambda parameters are final
   * (used in {@link TaxonomyInterpreter}.
   */
  public static void adapt(NameUsageMatch2 nameUsageMatch2, TaxonRecord taxonRecord) {
    Objects.requireNonNull(nameUsageMatch2);
    adaptInternal(nameUsageMatch2, taxonRecord);
  }

  private static TaxonRecord adaptInternal(NameUsageMatch2 source, TaxonRecord taxonRecord) {
    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(adaptRankedName(source.getUsage()));
    taxonRecord.setAcceptedUsage(adaptRankedName(source.getAcceptedUsage()));
    taxonRecord.setNomenclature(adaptNomenclature(source.getNomenclature()));
    taxonRecord.setClassification(source.getClassification()
                                    .stream()
                                    .map(TaxonRecordConverter::adaptRankedName)
                                    .collect(Collectors.toList()));
    taxonRecord.setDiagnostics(adaptDiagnostics(source.getDiagnostics()));

    return taxonRecord;
  }

  private static RankedName adaptRankedName(org.gbif.api.v2.RankedName rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    return RankedName.newBuilder()
      .setKey(rankedNameApi.getKey())
      .setName(rankedNameApi.getName())
      .setRank(Rank.valueOf(rankedNameApi.getRank().name()))
      .build();
  }

  private static Nomenclature adaptNomenclature(NameUsageMatch2.Nomenclature nomenclatureApi) {
    if (nomenclatureApi == null) {
      return null;
    }

    return Nomenclature.newBuilder().setId(nomenclatureApi.getId()).setSource(nomenclatureApi.getSource()).build();
  }

  private static Diagnostics adaptDiagnostics(NameUsageMatch2.Diagnostics diagnosticsApi) {
    if (diagnosticsApi == null) {
      return null;
    }

    List<TaxonRecord> alternatives = diagnosticsApi.getAlternatives()
      .stream()
      .map(nameUsageMatch -> adaptInternal(nameUsageMatch, new TaxonRecord()))
      .collect(Collectors.toList());

    return Diagnostics.newBuilder()
      .setAlternatives(alternatives)
      .setConfidence(diagnosticsApi.getConfidence())
      .setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()))
      .setNote(diagnosticsApi.getNote())
      .setStatus(Status.valueOf(diagnosticsApi.getStatus().name()))
      .setLineage(diagnosticsApi.getLineage())
      .build();
  }

}
