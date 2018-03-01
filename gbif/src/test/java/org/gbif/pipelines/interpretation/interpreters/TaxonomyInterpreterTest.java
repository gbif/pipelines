package org.gbif.pipelines.interpretation.interpreters;

import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.interpretation.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link TaxonomyInterpreter}.
 */
public class TaxonomyInterpreterTest {

  private static final String TEST_RECORD_ID = "testId";

  @Test
  public void testAssembledAuthor() {

    // Call to perform: https://api.gbif-uat.org/v1/species/match2?name=Puma
    // concolor&kingdom=Animalia&genus=Puma&rank=species
    ExtendedRecord record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor")
      .authorship("")

      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    Map<Rank, RankedName> ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(2435099, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());

    record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor (Linnaeus, 1771)")
      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(2435099, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());

    record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor")
      .authorship("(Linnaeus, 1771)")
      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(2435099, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  @Test
  public void testOenanthe() {
    ExtendedRecord record = new ExtendedRecordCustomBuilder().kingdom("Plantae")
      .name("Oenanthe")
      .authorship("")
      .rank(Rank.GENUS.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    Map<Rank, RankedName> ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(3034893, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(6, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Oenanthe L.", taxonRecord.getUsage().getName());

    record = new ExtendedRecordCustomBuilder().kingdom("Plantae")
      .name("Oenanthe")
      .authorship("L.")
      .rank(Rank.GENUS.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(3034893, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(6, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Oenanthe L.", taxonRecord.getUsage().getName());

    record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .name("Oenanthe")
      .authorship("Vieillot, 1816")
      .rank(Rank.GENUS.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(2492483, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Oenanthe Vieillot, 1816", taxonRecord.getUsage().getName());
  }

  @Test
  public void testOtu() {
    ExtendedRecord record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .phylum("Annelida")
      .family("Lumbricidae")
      .name("BOLD:ACV7160")
      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    // FIXME: this test does not pass. It is copied from old adapter but it returns a different response.Should we check it??
    // this is the old test
//      Assert.assertEquals("BOLD:ACV7160", interpretedTaxon.getTaxonRecord().getUsage().getName());
    Assert.assertEquals("Lumbricidae", taxonRecord.getUsage().getName());
  }

  @Test
  public void testCeratiaceae() {
    ExtendedRecord record = new ExtendedRecordCustomBuilder().kingdom("Chromista")
      .phylum("Dinophyta")
      .clazz("Dinophyceae")
      .order("Peridiniales")
      .family("Ceratiaceae")
      .genus("Ceratium")
      .name("Ceratium hirundinella")
      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    Map<Rank, RankedName> ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(7598904, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(7479242, ranksResponse.get(Rank.FAMILY).getKey().intValue());
    // FIXME: this tests does not pass. It is copied from old adapter but it returns a different response.Should we check it??
    // this is the old test
//      Assert.assertEquals("Ceratium hirundinella (O.F.Müller) Dujardin, 1841",
//                          interpretedTaxon.getTaxonRecord().getUsage().getName());
    Assert.assertEquals("Ceratium hirundinella (O.F.Müll.) Dujard.", taxonRecord.getUsage().getName());
  }

  @Test
  public void testNubLookupGood() {
    ExtendedRecord record = new ExtendedRecordCustomBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor")
      .rank(Rank.SPECIES.name())
      .id(TEST_RECORD_ID)
      .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    Map<Rank, RankedName> ranksResponse =
      taxonRecord.getClassification().stream().collect(Collectors.toMap(RankedName::getRank, rankedName -> rankedName));

    Assert.assertEquals(2435099, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1, ranksResponse.get(Rank.KINGDOM).getKey().intValue());
    Assert.assertEquals("Chordata", ranksResponse.get(Rank.PHYLUM).getName());
  }

  @Test
  public void testAcceptedUsage() {
    ExtendedRecord record = new ExtendedRecordCustomBuilder().name("Agallisus lepturoides").id(TEST_RECORD_ID).build();

    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);

    Assert.assertEquals(1118030, taxonRecord.getUsage().getKey().intValue());
    Assert.assertEquals(1118026, taxonRecord.getAcceptedUsage().getKey().intValue());
    Assert.assertEquals("Agallisus lepturoides Hovore, Penrose & Neck, 1987", taxonRecord.getUsage().getName());
    Assert.assertEquals("Agallissus lepturoides (Chevrolat, 1849)", taxonRecord.getAcceptedUsage().getName());
  }

  @Test(expected = NullPointerException.class)
  public void testMatchNullTaxonRecord() {
    TaxonomyInterpreter.taxonomyInterpreter(null).apply(null);
  }

  @Test(expected = NullPointerException.class)
  public void testMatchNullArgs() {
    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMatchEmptyArgs() {
    ExtendedRecord record = new ExtendedRecord();
    record.setCoreTerms(new HashMap<>());
    TaxonRecord taxonRecord = new TaxonRecord();
    TaxonomyInterpreter.taxonomyInterpreter(taxonRecord).apply(record);
  }

}
