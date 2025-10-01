package org.gbif.pipelines.core.interpreters.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse.Diagnostics;
import org.gbif.rest.client.species.NameUsageMatchResponse.MatchType;
import org.junit.Assert;
import org.junit.Test;

public class MultiTaxonomyInterpreterTest {

  /**
   * Isocountry or decimal latitude/longitude is not present in the er, so geocode webservices
   * should not be called, nameusage SHOULD NOT be called with the checklist key from the map
   */
  @Test
  public void checkGeocodeNotCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("ZA").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "fake-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(Map.of(DwcTerm.scientificName.qualifiedName(), "nonsense"))
            .build(),
        testRecord);

    Assert.assertFalse(geocodeCalled[0]);
    Assert.assertFalse(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is present in the er, so geocode webservices should not be called, nameusage SHOULD
   * be called with the checklist key from the map
   */
  @Test
  public void checkGeocodeNotCalledNameUsageWithChecklistCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("ZA").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.countryCode.qualifiedName(), "ZA"))
            .build(),
        testRecord);

    Assert.assertFalse(geocodeCalled[0]);
    Assert.assertTrue(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is present in the er, so geocode webservices should not be called, nameusage SHOULD
   * NOT be called as ISO code doesnt match anything in map
   */
  @Test
  public void checkGeocodeNotCalledNameUsageWithChecklistNotCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("ZA").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.countryCode.qualifiedName(), "US"))
            .build(),
        testRecord);

    Assert.assertFalse(geocodeCalled[0]);
    Assert.assertFalse(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is present in the er, so geocode webservices should not be called, nameusage SHOULD
   * NOT be called as ISO code doesnt match anything in map
   */
  @Test
  public void checkGeocodeNotCalledCountrySuppliedNameUsageWithChecklistNotCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("ZA").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.country.qualifiedName(), "UNITED STATES"))
            .build(),
        testRecord);

    Assert.assertFalse(geocodeCalled[0]);
    Assert.assertFalse(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is present in the er, so geocode webservices should not be called, nameusage SHOULD
   * NOT be called as ISO code doesnt match anything in map
   */
  @Test
  public void checkGeocodeNotCalledCountrySuppliedNameUsageWithChecklistCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("ZA").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.country.qualifiedName(), "South Africa"))
            .build(),
        testRecord);

    Assert.assertFalse(geocodeCalled[0]);
    Assert.assertTrue(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is not present in the er, so geocode webservices should not be called, nameusage
   * SHOULD be called with the checklist key from the map
   */
  @Test
  public void checkGeocodeCalledNameUsageWithChecklistNotCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(GeocodeResponse.Location.builder().isoCountryCode2Digit("US").build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.decimalLatitude.qualifiedName(), "88",
                    DwcTerm.decimalLongitude.qualifiedName(), "137"))
            .build(),
        testRecord);

    Assert.assertTrue(geocodeCalled[0]);
    Assert.assertFalse(nameUsageCalledWithChecklist[0]);
  }

  /**
   * Isocountry is present in the er, so geocode webservices should not be called, nameusage SHOULD
   * be called with the checklist key from the map
   */
  @Test
  public void checkGeocodeCalledNameUsageWithChecklistCalled() {

    // State
    NameUsageMatchResponse noMatch = new NameUsageMatchResponse();
    Diagnostics diagnostics = new Diagnostics();
    diagnostics.setMatchType(MatchType.NONE);
    noMatch.setDiagnostics(diagnostics);

    MultiTaxonRecord testRecord = MultiTaxonRecord.newBuilder().setId("not-an-id").build();

    final Boolean[] geocodeCalled = {false};
    final Boolean[] nameUsageCalledWithChecklist = {false};

    // When
    BiConsumer<ExtendedRecord, MultiTaxonRecord> consumer =
        MultiTaxonomyInterpreter.interpretMultiTaxonomy(
            new KeyValueStore<>() {

              @Override
              public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
                if (nameUsageMatchRequest.getChecklistKey().equals("za-checklist-key")) {
                  nameUsageCalledWithChecklist[0] = true;
                }
                return noMatch;
              }

              @Override
              public void close() throws IOException {}
            },
            new KeyValueStore<>() {
              @Override
              public GeocodeResponse get(GeocodeRequest nameUsageMatchRequest) {
                geocodeCalled[0] = true;
                GeocodeResponse r = new GeocodeResponse();
                r.setLocations(
                    List.of(
                        GeocodeResponse.Location.builder()
                            .name("South Africa")
                            .type("Political")
                            .isoCountryCode2Digit("ZA")
                            .build()));
                return r;
              }

              @Override
              public void close() throws IOException {}
            },
            List.of("fake-checklist-key"),
            Map.of("ZA", "za-checklist-key"));

    consumer.accept(
        ExtendedRecord.newBuilder()
            .setId("112345")
            .setCoreTerms(
                Map.of(
                    DwcTerm.scientificName.qualifiedName(), "nonsense",
                    DwcTerm.decimalLatitude.qualifiedName(), "88",
                    DwcTerm.decimalLongitude.qualifiedName(), "137"))
            .build(),
        testRecord);

    Assert.assertTrue(geocodeCalled[0]);
    Assert.assertTrue(nameUsageCalledWithChecklist[0]);
  }
}
