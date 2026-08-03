package org.gbif.pipelines.core.interpreters.extension;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;
import org.junit.Test;

public class DnaDerivedDataInterpreterTest {

  private static final VocabularyService vocabularyServiceFromFile =
      VocabularyService.builder()
          .vocabularyLookup(
              MixsTerm.target_gene.qualifiedName(),
              InMemoryVocabularyLookup.newBuilder()
                  .from(
                      Thread.currentThread()
                          .getContextClassLoader()
                          .getResourceAsStream("vocabs/target_gene.json"))
                  .build())
          .build();

  @Test
  public void dnaDerivedDataTest() throws JsonProcessingException {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> dnaDerivedData = new HashMap<>(2);
    final String seq1 = "ccacacctaaa __  aaactttccacgtgaacc";
    dnaDerivedData.put(GbifDnaTerm.dna_sequence.qualifiedName(), seq1);
    dnaDerivedData.put(MixsTerm.target_gene.qualifiedName(), "test");

    final String seq2 = "ccacacct";
    Map<String, String> dnaDerivedData2 = new HashMap<>(1);
    dnaDerivedData2.put(GbifDnaTerm.dna_sequence.qualifiedName(), seq2);

    ext.put(
        "http://rs.gbif.org/terms/1.0/DNADerivedData", List.of(dnaDerivedData, dnaDerivedData2));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    DnaDerivedDataRecord dr = DnaDerivedDataRecord.newBuilder().setId("id").build();

    // When
    DnaDerivedDataInterpreter.builder()
        .vocabularyService(vocabularyServiceFromFile)
        .dnaConfig(new DnaConfig())
        .create()
        .interpret(er, dr);

    // Should
    assertEquals(2, dr.getDnaDerivedDataItems().size());

    DnaDerivedData interpreted1 =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seq1))
            .findFirst()
            .get();
    assertEquals(seq1, interpreted1.getRawSequence());
    assertTrue(interpreted1.getInvalid());
    assertTrue(interpreted1.getGapsOrWhitespaceRemoved());
    assertNotNull(interpreted1.getNFraction());
    assertNotNull(interpreted1.getNRunsCapped());

    DnaDerivedData interpreted2 =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seq2))
            .findFirst()
            .get();
    assertEquals(seq2, interpreted2.getRawSequence());
    assertFalse(interpreted2.getInvalid());
    assertFalse(interpreted2.getGapsOrWhitespaceRemoved());
  }

  @Test
  public void deduplicateDnaDerivedDataTest() throws JsonProcessingException {
    // State - create duplicate rows with same interpreted nucleotideSequenceID and targetGene
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> dnaDerivedData1 = new HashMap<>(2);
    final String seqValid1 = "ccacacct";
    final String seqValid2 =
        "GGGGATATGGGGTACCGTCAAGTCCTTTGGGTTTTAAGCTTGGCTCGTAGTTCCCTGGCGATTTAGTGTAAATAAAAGTTTACGGCTGG";
    final String seqValid3 =
        "GGGGGTCTAAGGCACCGCCAAGTCCTTTGGGTTTTAAGCTAACGCTCGTAGTACCCGGGCGGACGTTTATAGTGGTATAACGTCTAGGTTTACGGCTGA";
    // invalid sequences, set to null during interpretation
    final String seqInvalid1 = "aaaa";
    final String seqInvalid2 = "bbbb";
    final String seqInvalid3 = "cccc";

    dnaDerivedData1.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid1);
    dnaDerivedData1.put(MixsTerm.target_gene.qualifiedName(), "test");

    Map<String, String> dnaDerivedData2 = new HashMap<>(2);
    dnaDerivedData2.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid1);
    dnaDerivedData2.put(MixsTerm.target_gene.qualifiedName(), "test2");

    Map<String, String> dnaDerivedData3 = new HashMap<>(2);
    dnaDerivedData3.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqInvalid1);
    dnaDerivedData3.put(MixsTerm.target_gene.qualifiedName(), "test");

    Map<String, String> dnaDerivedData4 = new HashMap<>(1);
    dnaDerivedData4.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid2);

    Map<String, String> dnaDerivedData5 = new HashMap<>(1);
    dnaDerivedData5.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid2);

    Map<String, String> dnaDerivedData6 = new HashMap<>(1);
    dnaDerivedData6.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqInvalid2);

    Map<String, String> dnaDerivedData7 = new HashMap<>(2);
    dnaDerivedData7.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid3);
    dnaDerivedData7.put(MixsTerm.target_gene.qualifiedName(), "RNA5S");

    Map<String, String> dnaDerivedData8 = new HashMap<>(2);
    dnaDerivedData8.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqValid3);
    dnaDerivedData8.put(MixsTerm.target_gene.qualifiedName(), "5S");

    Map<String, String> dnaDerivedData9 = new HashMap<>(2);
    dnaDerivedData9.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqInvalid3);
    dnaDerivedData9.put(MixsTerm.target_gene.qualifiedName(), "RNA5S");

    Map<String, String> dnaDerivedData10 = new HashMap<>(2);
    dnaDerivedData10.put(GbifDnaTerm.dna_sequence.qualifiedName(), seqInvalid3);
    dnaDerivedData10.put(MixsTerm.target_gene.qualifiedName(), "5S");

    ext.put(
        "http://rs.gbif.org/terms/1.0/DNADerivedData",
        List.of(
            dnaDerivedData1,
            dnaDerivedData2,
            dnaDerivedData3,
            dnaDerivedData4,
            dnaDerivedData5,
            dnaDerivedData6,
            dnaDerivedData7,
            dnaDerivedData8,
            dnaDerivedData9,
            dnaDerivedData10));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    DnaDerivedDataRecord dr = DnaDerivedDataRecord.newBuilder().setId("id").build();

    // When
    DnaDerivedDataInterpreter.builder()
        .vocabularyService(vocabularyServiceFromFile)
        .dnaConfig(new DnaConfig())
        .create()
        .interpret(er, dr);

    // Should - duplicates should be removed, leaving 5 items
    assertEquals(5, dr.getDnaDerivedDataItems().size());

    long seqValid1Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqValid1))
            .count();
    assertEquals(1, seqValid1Count);

    long seqValid2Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqValid2))
            .count();
    assertEquals(1, seqValid2Count);

    long seqValid3Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqValid3))
            .count();
    assertEquals(1, seqValid3Count);

    long seqInvalid1Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqInvalid1))
            .count();
    assertEquals(1, seqInvalid1Count);

    long seqInvalid2Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqInvalid2))
            .count();
    assertEquals(0, seqInvalid2Count);

    long seqInvalid3Count =
        dr.getDnaDerivedDataItems().stream()
            .filter(d -> d.getRawSequence().equalsIgnoreCase(seqInvalid3))
            .count();
    assertEquals(1, seqInvalid3Count);

    // Should - a duplicate issue should be added
    assertTrue(
        "DUPLICATE_NUCLEOTIDE_SEQUENCES_COLLAPSED issue should be present",
        dr.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.DUPLICATE_NUCLEOTIDE_SEQUENCES_COLLAPSED.name()));
  }
}
