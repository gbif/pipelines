package org.gbif.pipelines.transforms.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class MeasurementOrFactTableTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void noExtesnsionTest() {

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();
    GbifIdRecord id = GbifIdRecord.newBuilder().setId("777").setGbifId(777L).build();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().create();

    MeasurementOrFactTableTransform transform =
        MeasurementOrFactTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .gbifIdRecordTag(gbifIdTransform.getTag())
            .build();

    // When
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Create er", Create.of(er)).apply("KV er", verbatimTransform.toKv());

    PCollection<KV<String, GbifIdRecord>> basicCollection =
        p.apply("Create id", Create.of(id)).apply("KV id", gbifIdTransform.toKv());

    PCollection<MeasurementOrFactTable> result =
        KeyedPCollectionTuple
            // Core
            .of(gbifIdTransform.getTag(), basicCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging", transform.convert());

    // Should
    PAssert.that(result).empty();
    p.run();
  }

  @Test
  public void measurementOrFactTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setExtensions(ext).build();
    GbifIdRecord id = GbifIdRecord.newBuilder().setId("777").setGbifId(777L).build();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().create();
    MeasurementOrFactTableTransform transform =
        MeasurementOrFactTableTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .gbifIdRecordTag(gbifIdTransform.getTag())
            .build();

    // When
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Create er", Create.of(er)).apply("KV er", verbatimTransform.toKv());

    PCollection<KV<String, GbifIdRecord>> basicCollection =
        p.apply("Create id", Create.of(id)).apply("KV id", gbifIdTransform.toKv());

    PCollection<MeasurementOrFactTable> result =
        KeyedPCollectionTuple
            // Core
            .of(gbifIdTransform.getTag(), basicCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging", transform.convert());

    // Should
    MeasurementOrFactTable expected =
        MeasurementOrFactTable.newBuilder()
            .setGbifid("777")
            .setVMeasurementid("Id1")
            .setVMeasurementtype("Type1")
            .setVMeasurementvalue("1.5")
            .setVMeasurementaccuracy("Accurancy1")
            .setVMeasurementunit("Unit1")
            .setVMeasurementdeterminedby("By1")
            .setVMeasurementmethod("Method1")
            .setVMeasurementremarks("Remarks1")
            .setVMeasurementdetermineddate("2010/2011")
            .setMeasurementid("Id1")
            .setMeasurementtype("Type1")
            .setMeasurementvalue("1.5")
            .setMeasurementaccuracy("Accurancy1")
            .setMeasurementunit("Unit1")
            .setMeasurementdeterminedby("By1")
            .setMeasurementmethod("Method1")
            .setMeasurementremarks("Remarks1")
            .setMeasurementdetermineddate("2010/2011")
            .build();

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }
}
