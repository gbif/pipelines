package org.gbif.pipelines.taxonomy.interpreter;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.adapter.RecordAdapter;
import org.gbif.pipelines.io.avro.Diagnostics;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link RecordAdapter} that adapts a {@link NameUsageMatch2} into a {@link TaxonRecord}
 */
public class TaxonRecordAdapter implements RecordAdapter<NameUsageMatch2, TaxonRecord> {

  @Override
  public TaxonRecord adapt(NameUsageMatch2 source) {
    return adaptInternal(source);
  }

  private static TaxonRecord adaptInternal(NameUsageMatch2 source) {
    Objects.requireNonNull(source);

    // not convenient to use the builder here because it throws a runtime when handling null values
    TaxonRecord taxonRecord = new TaxonRecord();

    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(adaptRankedName(source.getUsage()));
    taxonRecord.setAcceptedUsage(adaptRankedName(source.getAcceptedUsage()));
    taxonRecord.setNomenclature(adaptNomenclature(source.getNomenclature()));
    taxonRecord.setClassification(source.getClassification()
                                    .stream()
                                    .map(TaxonRecordAdapter::adaptRankedName)
                                    .collect(Collectors.toList()));
    taxonRecord.setDiagnostics(adaptDiagnostics(source.getDiagnostics()));

    return taxonRecord;
  }

  private static RankedName adaptRankedName(org.gbif.api.v2.RankedName rankedNameApi) {
    return rankedNameApi != null ? RankedName.newBuilder()
      .setKey(rankedNameApi.getKey())
      .setName(rankedNameApi.getName())
      .setRank(Rank.valueOf(rankedNameApi.getRank().name()))
      .build() : null;
  }

  private static Nomenclature adaptNomenclature(NameUsageMatch2.Nomenclature nomenclatureApi) {
    return nomenclatureApi != null ? Nomenclature.newBuilder()
      .setId(nomenclatureApi.getId())
      .setSource(nomenclatureApi.getSource())
      .build() : null;
  }

  private static Diagnostics adaptDiagnostics(NameUsageMatch2.Diagnostics diagnosticsApi) {
    return diagnosticsApi != null ? Diagnostics.newBuilder()
      .setAlternatives(diagnosticsApi.getAlternatives()
                         .stream()
                         .map(TaxonRecordAdapter::adaptInternal)
                         .collect(Collectors.toList()))
      .setConfidence(diagnosticsApi.getConfidence())
      .setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()))
      .setNote(diagnosticsApi.getNote())
      .setStatus(Status.valueOf(diagnosticsApi.getStatus().name()))
      .setLineage(diagnosticsApi.getLineage()
                    .stream()
                    .map(lineage -> (CharSequence) lineage)
                    .collect(Collectors.toList()))
      .build() : null;
  }
}
