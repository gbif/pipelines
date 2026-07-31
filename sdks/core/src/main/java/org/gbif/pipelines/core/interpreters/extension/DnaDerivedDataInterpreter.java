package org.gbif.pipelines.core.interpreters.extension;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;
import org.gbif.api.util.DnaUtils;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dna.core.SequenceProcessor;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Builder(buildMethodName = "create")
@Slf4j
public class DnaDerivedDataInterpreter {

  private final VocabularyService vocabularyService;
  @Builder.Default private final DnaConfig dnaConfig = new DnaConfig();

  /**
   * Interprets DNA data of a {@link ExtendedRecord} and populates a {@link DnaDerivedDataRecord}
   * with the interpreted values.
   */
  public void interpret(ExtendedRecord er, DnaDerivedDataRecord dr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(dr);

    ExtensionInterpretation.Result<DnaDerivedData> result =
        ExtensionInterpretation.extension(Extension.DNA_DERIVED_DATA)
            .to(DnaDerivedData::new)
            .map(GbifDnaTerm.dna_sequence, this::interpretSequence)
            .map(MixsTerm.target_gene, this::interpretTargetGene)
            .convert(er);

    DeduplicationResult dedupResult = deduplicateDnaDerivedData(result.getList());
    dr.setDnaDerivedDataItems(dedupResult.dedupedItems);
    if (result.getIssues() != null) {
      dr.getIssues().getIssueList().addAll(result.getIssuesAsList());
    }
    if (dedupResult.hasDuplicates) {
      // TODO: create issue in gbif-api
      dr.getIssues().getIssueList().add("DNA_DERIVED_DATA_DUPLICATE");
    }
  }

  private List<String> interpretTargetGene(DnaDerivedData dnaDerivedData, String rawValue) {
    List<String> issues = new ArrayList<>();
    VocabularyInterpreter.interpretVocabulary(
            MixsTerm.target_gene,
            rawValue,
            vocabularyService,
            v -> issues.add(OccurrenceIssue.TARGET_GENE_INVALID.name()))
        .ifPresent(dnaDerivedData::setTargetGene);

    return issues;
  }

  private List<String> interpretSequence(DnaDerivedData dnaDerivedData, String rawValue) {
    List<String> issues = new ArrayList<>();
    if (!Strings.isNullOrEmpty(rawValue)) {
      SequenceProcessor sequenceProcessor = new SequenceProcessor();
      SequenceProcessor.Result result = sequenceProcessor.processOneSequence(rawValue);

      dnaDerivedData.setDnaSequenceID(DnaUtils.convertDnaSequenceToID(rawValue));
      dnaDerivedData.setRawSequence(rawValue);
      dnaDerivedData.setNucleotideSequenceID(result.nucleotideSequenceID());
      dnaDerivedData.setSequence(result.sequence());
      dnaDerivedData.setSequenceLength(result.sequenceLength());
      dnaDerivedData.setGcContent(result.gcContent());
      dnaDerivedData.setNonIupacFraction(result.nonIupacFraction());
      dnaDerivedData.setNonACGTNFraction(result.nonACGTNFraction());
      dnaDerivedData.setNFraction(result.nFraction());
      dnaDerivedData.setNRunsCapped(result.nRunsCapped());
      dnaDerivedData.setNaturalLanguageDetected(result.naturalLanguageDetected());
      dnaDerivedData.setEndsTrimmed(result.endsTrimmed());
      dnaDerivedData.setGapsOrWhitespaceRemoved(result.gapsOrWhitespaceRemoved());
      dnaDerivedData.setInvalid(result.invalid());

      if (result.naturalLanguageDetected()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_NATURAL_LANGUAGE.name());
      }

      if (result.endsTrimmed()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_ENDS_TRIMMED.name());
      }

      if (result.gapsOrWhitespaceRemoved()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_GAPS_REMOVED.name());
      }

      if (result.invalid()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_INVALID.name());
      }

      if (result.nFraction() != null
          && result.nFraction() > dnaConfig.getNucleotideSequenceHighNFractionThreshold()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_HIGH_N_FRACTION.name());
      }

      if (result.nonACGTNFraction() != null
          && result.nonACGTNFraction() > dnaConfig.getNucleotideSequenceHighAmbiguityThreshold()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_HIGH_AMBIGUITY.name());
      }
    }

    return issues;
  }

  private DeduplicationResult deduplicateDnaDerivedData(List<DnaDerivedData> items) {
    if (items == null || items.isEmpty()) {
      return new DeduplicationResult(items, false);
    }

    Map<String, DnaDerivedData> deduplicatedMap = new LinkedHashMap<>();
    boolean hasDuplicates = false;
    for (DnaDerivedData item : items) {
      String key = createDeduplicationKey(item);
      if (deduplicatedMap.containsKey(key)) {
        hasDuplicates = true;
      }
      deduplicatedMap.putIfAbsent(key, item);
    }

    return new DeduplicationResult(new ArrayList<>(deduplicatedMap.values()), hasDuplicates);
  }

  private String createDeduplicationKey(DnaDerivedData item) {
    String nucleotideSequenceID =
        item.getNucleotideSequenceID() != null ? item.getNucleotideSequenceID() : "";
    String targetGene = "";
    if (item.getTargetGene() != null && item.getTargetGene().getConcept() != null) {
      targetGene = item.getTargetGene().getConcept();
    }
    return nucleotideSequenceID + "|" + targetGene;
  }

  private static class DeduplicationResult {
    final List<DnaDerivedData> dedupedItems;
    final boolean hasDuplicates;

    DeduplicationResult(List<DnaDerivedData> dedupedItems, boolean hasDuplicates) {
      this.dedupedItems = dedupedItems;
      this.hasDuplicates = hasDuplicates;
    }
  }
}
