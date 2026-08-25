package org.gbif.pipelines.estools.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class EsQueryBoostsTest {

  @Test
  public void occurrenceDefaultFieldsKeepTaxonBoosts() {
    String setting = EsQueryBoosts.defaultFieldSetting("elasticsearch/es-occurrence-schema.json");
    assertEquals(
        "[\"all\",\"taxonID^100\",\"taxonConceptID^100\",\"verbatimScientificName^100\"]", setting);
    assertFalse(EsQueryBoosts.isEventSchema("elasticsearch/es-occurrence-schema.json"));
  }

  @Test
  public void eventDefaultFieldsKeepTaxonIdsBoost() {
    String setting = EsQueryBoosts.defaultFieldSetting("elasticsearch/es-event-schema.json");
    assertEquals("[\"all\",\"taxonIDs^100\"]", setting);
    assertTrue(EsQueryBoosts.isEventSchema("elasticsearch/es-event-schema.json"));
  }
}
