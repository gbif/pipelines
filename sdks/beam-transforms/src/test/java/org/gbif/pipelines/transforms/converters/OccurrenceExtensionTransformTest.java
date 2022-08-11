package org.gbif.pipelines.transforms.converters;

import static org.gbif.dwc.terms.DwcTerm.MeasurementOrFact;
import static org.gbif.dwc.terms.DwcTerm.Occurrence;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class OccurrenceExtensionTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void extensionContainsOccurrenceTest() {

    // State
    String coreId = "1";
    String extId = "2";
    String ext2Id = "3";
    String ext3Id = "4";
    String somethingCore = "somethingCore";
    String somethingCoreExt = "somethingCoreExt";
    String somethingExt = "somethingExt";

    // Core
    Map<String, String> core = new HashMap<>(2);
    core.put(DwcTerm.occurrenceID.qualifiedName(), coreId);
    core.put(somethingCore, somethingCore);

    // Occurrence
    Map<String, String> occExt1 = new HashMap<>(2);
    occExt1.put(DwcTerm.occurrenceID.qualifiedName(), extId);
    occExt1.put(somethingCoreExt, somethingCoreExt);

    Map<String, String> occExt2 = new HashMap<>(2);
    occExt2.put(DwcTerm.occurrenceID.qualifiedName(), ext2Id);
    occExt2.put(somethingCoreExt, somethingCoreExt);

    Map<String, String> occExt3 = new HashMap<>(2);
    occExt3.put(DwcTerm.occurrenceID.qualifiedName(), ext3Id);
    occExt3.put(somethingCoreExt, somethingCoreExt);

    // MeasurementOrFact
    Map<String, String> mediaExt1 = new HashMap<>(2);
    mediaExt1.put(DwcTerm.occurrenceID.qualifiedName(), extId);
    mediaExt1.put(somethingExt, somethingExt);

    Map<String, String> mediaExt2 = new HashMap<>(2);
    mediaExt2.put(DwcTerm.occurrenceID.qualifiedName(), ext2Id);
    mediaExt2.put(somethingExt, somethingExt);

    Map<String, String> mediaExt3 = new HashMap<>(2);
    mediaExt3.put(DwcTerm.occurrenceID.qualifiedName(), ext3Id);
    mediaExt3.put(somethingExt, somethingExt);

    // Map
    Map<String, List<Map<String, String>>> extensions = new HashMap<>(2);
    extensions.put(Occurrence.qualifiedName(), Arrays.asList(occExt1, occExt2, occExt3));
    extensions.put(
        MeasurementOrFact.qualifiedName(), Arrays.asList(mediaExt1, mediaExt2, mediaExt3));

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(coreId)
            .setCoreTerms(core)
            .setExtensions(extensions)
            .build();

    final List<ExtendedRecord> expected =
        createCollection(
            coreId,
            true,
            extId + "_" + somethingCore + "_" + somethingCoreExt + "_" + somethingExt,
            ext2Id + "_" + somethingCore + "_" + somethingCoreExt + "_" + somethingExt,
            ext3Id + "_" + somethingCore + "_" + somethingCoreExt + "_" + somethingExt);

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(er)).apply(OccurrenceExtensionTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void occurrenceExtensionIsEmptyTest() {

    // State
    String id = "1";
    String somethingCore = "somethingCore";

    Map<String, String> ext = new HashMap<>(2);
    ext.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext.put(somethingCore, somethingCore);

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(ext)
            .setExtensions(
                Collections.singletonMap(Occurrence.qualifiedName(), Collections.emptyList()))
            .build();

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(er)).apply(OccurrenceExtensionTransform.create());

    // Should
    PAssert.that(result).empty();
    p.run();
  }

  @Test
  public void noOccurrenceExtensionTest() {

    // State
    String id = "1";
    String somethingCore = "somethingCore";

    Map<String, String> ext = new HashMap<>(2);
    ext.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext.put(somethingCore, somethingCore);

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(id).setCoreTerms(ext).build();

    final List<ExtendedRecord> expected = createCollection(null, false, id + "_" + somethingCore);

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(er)).apply(OccurrenceExtensionTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void eventNoOccurrenceExtensionTest() {

    // State
    String id = "1";
    String somethingCore = "somethingCore";

    Map<String, String> ext = new HashMap<>(2);
    ext.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext.put(somethingCore, somethingCore);

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreRowType("non-occurrence")
            .setCoreTerms(ext)
            .build();

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(er)).apply(OccurrenceExtensionTransform.create());

    // Should
    PAssert.that(result).empty();
    p.run();
  }

  private List<ExtendedRecord> createCollection(String coreId, boolean isExt, String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");

              Map<String, String> core = new HashMap<>(3);
              core.put(DwcTerm.occurrenceID.qualifiedName(), array[0]);
              core.put(array[1], array[1]);
              if (array.length > 3) {
                core.put(array[2], array[2]);
              }

              ExtendedRecord.Builder builder =
                  ExtendedRecord.newBuilder().setCoreId(coreId).setId(array[0]).setCoreTerms(core);

              if (isExt && array.length == 4) {
                Map<String, String> ext = new HashMap<>(32);
                ext.put(DwcTerm.occurrenceID.qualifiedName(), array[0]);
                ext.put(array[3], array[3]);
                builder.setExtensions(
                    Collections.singletonMap(
                        MeasurementOrFact.qualifiedName(), Collections.singletonList(ext)));
              }

              return builder.build();
            })
        .collect(Collectors.toList());
  }
}
