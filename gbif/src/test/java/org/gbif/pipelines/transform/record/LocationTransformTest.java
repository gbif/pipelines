package org.gbif.pipelines.transform.record;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.transform.Kv2Value;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LocationTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    final String[] denmark = {"0", "DENMARK", "DK", "EUROPE", "100", "110", "111", "200", "Ocean"};
    final String[] japan = {"1", "JAPAN", "JP", "ASIA", "100", "110", "111", "200", "Ocean"};
    final List<ExtendedRecord> records = createExtendedRecordList(denmark, japan);

    // Expected
    final List<Location> locations = createLocationList(denmark, japan);

    // When
    LocationTransform locationTransform = new LocationTransform().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(records));

    PCollectionTuple tuple = inputStream.apply(locationTransform);

    PCollection<Location> recordCollection = tuple.get(locationTransform.getDataTag()).apply(Kv2Value.create());

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(locations);
    p.run();

  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... locations) {
    return Arrays.stream(locations).map(x -> {
      ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
      record.getCoreTerms().put(DwcTerm.country.qualifiedName(), x[1]);
      record.getCoreTerms().put(DwcTerm.countryCode.qualifiedName(), x[2]);
      record.getCoreTerms().put(DwcTerm.continent.qualifiedName(), x[3]);
      record.getCoreTerms().put(DwcTerm.minimumElevationInMeters.qualifiedName(), x[4]);
      record.getCoreTerms().put(DwcTerm.maximumElevationInMeters.qualifiedName(), x[5]);
      record.getCoreTerms().put(DwcTerm.minimumDepthInMeters.qualifiedName(), x[6]);
      record.getCoreTerms().put(DwcTerm.maximumDepthInMeters.qualifiedName(), x[7]);
      record.getCoreTerms().put(DwcTerm.waterBody.qualifiedName(), x[8]);
      return record;
    }).collect(Collectors.toList());
  }

  private List<Location> createLocationList(String[]... locations) {
    return Arrays.stream(locations)
      .map(x -> Location.newBuilder()
        .setOccurrenceID(x[0])
        .setCountry(x[1])
        .setCountryCode(x[1])
        .setContinent(x[3])
        .setMinimumElevationInMeters(x[4])
        .setMaximumElevationInMeters(x[5])
        .setMinimumDepthInMeters(x[6])
        .setMaximumDepthInMeters(x[7])
        .setWaterBody(x[8])
        .build())
      .collect(Collectors.toList());
  }

}
