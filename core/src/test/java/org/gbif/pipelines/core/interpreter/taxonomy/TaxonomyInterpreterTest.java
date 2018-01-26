package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.core.utils.ExtendedRecordBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link TaxonomyInterpreter}.
 */
public class TaxonomyInterpreterTest {

  @Test
  public void testAssembledAuthor() {

    // Call to perform: https://api.gbif-uat.org/v1/species/match2?name=Puma
    // concolor&kingdom=Animalia&genus=Puma&rank=species
    ExtendedRecord record = new ExtendedRecordBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor")
      .authorship("")
      .rank(Rank.SPECIES.name())
      .build();

    InterpretedTaxonomy interpretedTaxon = null;
    try {

      interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(2435099, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Chordata", interpretedTaxon.getTaxonRecord().getPhylum());

      record = new ExtendedRecordBuilder().kingdom("Animalia")
        .genus("Puma")
        .name("Puma concolor (Linnaeus, 1771)")
        .rank(Rank.SPECIES.name())
        .build();

      interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(2435099, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Chordata", interpretedTaxon.getTaxonRecord().getPhylum());

      record = new ExtendedRecordBuilder().kingdom("Animalia")
        .genus("Puma")
        .name("Puma concolor")
        .authorship("(Linnaeus, 1771)")
        .rank(Rank.SPECIES.name())
        .build();

      interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(2435099, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Chordata", interpretedTaxon.getTaxonRecord().getPhylum());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testOenanthe() {
    ExtendedRecord record =
      new ExtendedRecordBuilder().kingdom("Plantae").name("Oenanthe").authorship("").rank(Rank.GENUS.name()).build();

    try {

      InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(3034893, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(6, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Oenanthe L.", interpretedTaxon.getTaxonRecord().getUsageName());

      record = new ExtendedRecordBuilder().kingdom("Plantae")
        .name("Oenanthe")
        .authorship("L.")
        .rank(Rank.GENUS.name())
        .build();

      interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(3034893, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(6, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Oenanthe L.", interpretedTaxon.getTaxonRecord().getUsageName());

      record = new ExtendedRecordBuilder().kingdom("Animalia")
        .name("Oenanthe")
        .authorship("Vieillot, 1816")
        .rank(Rank.GENUS.name())
        .build();

      interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(2492483, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Oenanthe Vieillot, 1816", interpretedTaxon.getTaxonRecord().getUsageName());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testOtu() {
    ExtendedRecord record = new ExtendedRecordBuilder().kingdom("Animalia")
      .phylum("Annelida")
      .family("Lumbricidae")
      .name("BOLD:ACV7160")
      .rank(Rank.SPECIES.name())
      .build();

    try {
      InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals("BOLD:ACV7160", interpretedTaxon.getTaxonRecord().getUsageName());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testCeratiaceae() {
    ExtendedRecord record = new ExtendedRecordBuilder().kingdom("Chromista")
      .phylum("Dinophyta")
      .clazz("Dinophyceae")
      .order("Peridiniales")
      .family("Ceratiaceae")
      .genus("Ceratium")
      .name("Ceratium hirundinella")
      .rank(Rank.SPECIES.name())
      .build();

    try {
      InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(7598904, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(7479242, interpretedTaxon.getTaxonRecord().getFamilyKey().intValue());
      Assert.assertEquals("Ceratium hirundinella (O.F.Müller) Dujardin, 1841",
                          interpretedTaxon.getTaxonRecord().getUsageName());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void testNubLookupGood() {
    ExtendedRecord record = new ExtendedRecordBuilder().kingdom("Animalia")
      .genus("Puma")
      .name("Puma concolor")
      .rank(Rank.SPECIES.name())
      .build();

    try {
      InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(2435099, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1, interpretedTaxon.getTaxonRecord().getKingdomKey().intValue());
      Assert.assertEquals("Chordata", interpretedTaxon.getTaxonRecord().getPhylum());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void testAcceptedUsage() {
    ExtendedRecord record = new ExtendedRecordBuilder().name("Agallisus lepturoides").build();

    try {
      InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);

      Assert.assertEquals(1118030, interpretedTaxon.getTaxonRecord().getUsageKey().intValue());
      Assert.assertEquals(1118026, interpretedTaxon.getTaxonRecord().getAcceptedUsageKey().intValue());
      Assert.assertEquals("Agallisus lepturoides Hovore, Penrose & Neck, 1987", interpretedTaxon.getTaxonRecord()
        .getUsageName());
      Assert.assertEquals("Agallissus lepturoides (Chevrolat, 1849)", interpretedTaxon.getTaxonRecord()
        .getAcceptedUsageName());

    } catch (TaxonomyInterpretationException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test(expected = TaxonomyInterpretationException.class)
  public void testMatchNullArgs() throws TaxonomyInterpretationException {
    InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(new ExtendedRecord());
  }

  @Test(expected = TaxonomyInterpretationException.class)
  public void testMatchEmptyArgs() throws TaxonomyInterpretationException {
    ExtendedRecord record = new ExtendedRecord();
    record.setCoreTerms(new HashMap<>());
    InterpretedTaxonomy interpretedTaxon = TaxonomyInterpreter.interpretTaxonomyFields(record);
  }

}
