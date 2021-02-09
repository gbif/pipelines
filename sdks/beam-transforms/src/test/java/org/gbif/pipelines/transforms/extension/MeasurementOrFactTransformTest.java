package org.gbif.pipelines.transforms.extension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class MeasurementOrFactTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate
      extends DoFn<MeasurementOrFactRecord, MeasurementOrFactRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      MeasurementOrFactRecord record =
          MeasurementOrFactRecord.newBuilder(context.element()).build();
      record.setCreated(null);
      context.output(record);
    }
  }

  private static final String RECORD_ID = "777";

  @Test
  public void transformationExtAndDynamicTest() {

    // Expected
    MeasurementOrFact length =
        MeasurementOrFact.newBuilder()
            .setValue("495")
            .setUnit("mm")
            .setType("total length")
            .build();

    MeasurementOrFact mass =
        MeasurementOrFact.newBuilder().setValue("100").setUnit("g").setType("total weight").build();

    MeasurementOrFact ext =
        MeasurementOrFact.newBuilder().setValue("ext").setUnit("ext").setType("ext").build();

    List<MeasurementOrFact> mfcList = Arrays.asList(ext, length, mass);
    MeasurementOrFactRecord mfc =
        MeasurementOrFactRecord.newBuilder()
            .setId(RECORD_ID)
            .setMeasurementOrFactItems(mfcList)
            .build();
    List<MeasurementOrFactRecord> result = Collections.singletonList(mfc);

    // State
    ExtendedRecord extendedRecord =
        getExtended(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;weight=100g", "ext");

    // When
    PCollection<MeasurementOrFactRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(MeasurementOrFactTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(result);
    p.run();
  }

  @Test
  public void transformationDynamicTest() {

    // Expected
    MeasurementOrFact length =
        MeasurementOrFact.newBuilder()
            .setValue("495")
            .setUnit("mm")
            .setType("total length")
            .build();

    MeasurementOrFact mass =
        MeasurementOrFact.newBuilder().setValue("100").setUnit("g").setType("total weight").build();

    List<MeasurementOrFact> mfcList = Arrays.asList(length, mass);
    MeasurementOrFactRecord mfc =
        MeasurementOrFactRecord.newBuilder()
            .setId(RECORD_ID)
            .setMeasurementOrFactItems(mfcList)
            .build();
    List<MeasurementOrFactRecord> result = Collections.singletonList(mfc);

    // State
    ExtendedRecord extendedRecord =
        getExtended(
            "sex=female;age class=adult;total length=495 mm;tail length=210 mm;weight=100g", null);

    // When
    PCollection<MeasurementOrFactRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(MeasurementOrFactTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(result);
    p.run();
  }

  @Test
  public void transformationExtTest() {

    // Expected
    MeasurementOrFact ext =
        MeasurementOrFact.newBuilder().setValue("ext").setUnit("ext").setType("ext").build();

    MeasurementOrFactRecord mfc =
        MeasurementOrFactRecord.newBuilder()
            .setId(RECORD_ID)
            .setMeasurementOrFactItems(Collections.singletonList(ext))
            .build();
    List<MeasurementOrFactRecord> result = Collections.singletonList(mfc);

    // State
    ExtendedRecord extendedRecord = getExtended(null, "ext");

    // When
    PCollection<MeasurementOrFactRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(MeasurementOrFactTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(result);
    p.run();
  }

  @Test
  public void transformationEmptyTest() {

    // State
    ExtendedRecord extendedRecord = getExtended(null, null);

    // When
    PCollection<MeasurementOrFactRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(MeasurementOrFactTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).empty();
    p.run();
  }

  private ExtendedRecord getExtended(String dynamic, String extension) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(RECORD_ID);

    if (dynamic != null) {
      builder.setCoreTerms(
          Collections.singletonMap(DwcTerm.dynamicProperties.qualifiedName(), dynamic));
    }

    if (extension != null) {
      Map<String, String> map = new HashMap<>();
      map.put(DwcTerm.measurementType.qualifiedName(), extension);
      map.put(DwcTerm.measurementValue.qualifiedName(), extension);
      map.put(DwcTerm.measurementUnit.qualifiedName(), extension);

      builder.setExtensions(
          Collections.singletonMap(
              Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(map)));
    }

    return builder.build();
  }
}
