package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.interpreter.RecordAdapter;
import org.gbif.pipelines.io.avro.Diagnostics;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link RecordAdapter} that adapts a {@link NameUsageMatch2} into a {@link TaxonRecord}
 */
public class TaxonRecordAdapter implements RecordAdapter<NameUsageMatch2, TaxonRecord> {

  @Override
  public TaxonRecord adapt(NameUsageMatch2 source) {
    return adaptInternal(source);
  }

  private TaxonRecord adaptInternal(NameUsageMatch2 source) {
    Optional.ofNullable(source).orElseThrow(() -> new IllegalArgumentException("NameUsageMatch2 source is required"));

    TaxonRecord taxonRecord = new TaxonRecord();

    taxonRecord.setSynonym(source.isSynonym());
    taxonRecord.setUsage(adaptRankedName(source.getUsage()));
    taxonRecord.setAcceptedUsage(adaptRankedName(source.getAcceptedUsage()));
    taxonRecord.setNomenclature(adaptNomenclature(source.getNomenclature()));
    taxonRecord.setClassification(source.getClassification()
                                    .stream()
                                    .map(rankedName -> adaptRankedName(rankedName))
                                    .collect(Collectors.toList()));
    taxonRecord.setDiagnostics(adaptDiagnostics(source.getDiagnostics()));

    return taxonRecord;
  }

  private RankedName adaptRankedName(org.gbif.api.v2.RankedName rankedNameApi) {
    if (rankedNameApi == null) {
      return null;
    }

    RankedName rankedName = new RankedName();

    rankedName.setKey(rankedNameApi.getKey());
    rankedName.setName(rankedNameApi.getName());
    rankedName.setRank(Rank.valueOf(rankedNameApi.getRank().name()));

    return rankedName;
  }

  private Nomenclature adaptNomenclature(NameUsageMatch2.Nomenclature nomenclatureApi) {
    if (nomenclatureApi == null) {
      return null;
    }

    Nomenclature nomenclature = new Nomenclature();

    nomenclature.setId(nomenclature.getId());
    nomenclature.setSource(nomenclatureApi.getSource());

    return nomenclature;
  }

  private Diagnostics adaptDiagnostics(NameUsageMatch2.Diagnostics diagnosticsApi) {
    if (diagnosticsApi == null) {
      return null;
    }

    Diagnostics diagnostics = new Diagnostics();

    diagnostics.setAlternatives(diagnosticsApi.getAlternatives()
                                  .stream()
                                  .map(nameUsageMatch2 -> adaptInternal(nameUsageMatch2))
                                  .collect(Collectors.toList()));
    diagnostics.setConfidence(diagnosticsApi.getConfidence());
    diagnostics.setMatchType(MatchType.valueOf(diagnosticsApi.getMatchType().name()));
    diagnostics.setNote(diagnosticsApi.getNote());
    diagnostics.setStatus(Status.valueOf(diagnosticsApi.getStatus().name()));
    diagnostics.setLineage(diagnosticsApi.getLineage()
                             .stream()
                             .map(lineage -> (CharSequence) lineage)
                             .collect(Collectors.toList()));

    return diagnostics;
  }
}
