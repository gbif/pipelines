package org.gbif.pipelines.transforms.converters;

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
import org.gbif.pipelines.core.utils.HashUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class OccurrenceExtensionTransformTest {

  private final String datasetId = "e8608a74-5ff4-4669-abe9-8d893f10f8d3";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void extensionContainsOccurrenceTest() {

    // State
    String coreId = "1";
    String extId = "2";
    String ext2Id = "3";
    String ext3Id = "4";
    String somethingCore = "somethingCore";
    String somethingExt = "somethingExt";

    Map<String, String> core = new HashMap<>(2);
    core.put(DwcTerm.occurrenceID.qualifiedName(), coreId);
    core.put(somethingCore, somethingCore);

    Map<String, String> ext1 = new HashMap<>(2);
    ext1.put(DwcTerm.occurrenceID.qualifiedName(), extId);
    ext1.put(somethingExt, somethingExt);

    Map<String, String> ext2 = new HashMap<>(2);
    ext2.put(DwcTerm.occurrenceID.qualifiedName(), ext2Id);
    ext2.put(somethingExt, somethingExt);

    Map<String, String> ext3 = new HashMap<>(2);
    ext3.put(DwcTerm.occurrenceID.qualifiedName(), ext3Id);
    ext3.put(somethingExt, somethingExt);

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(coreId)
            .setCoreTerms(core)
            .setExtensions(
                Collections.singletonMap(
                    Occurrence.qualifiedName(), Arrays.asList(ext1, ext2, ext3)))
            .build();

    final List<ExtendedRecord> expected =
        createCollection(
            false,
            false,
            extId + "_" + somethingCore + "_" + somethingExt,
            ext2Id + "_" + somethingCore + "_" + somethingExt,
            ext3Id + "_" + somethingCore + "_" + somethingExt);

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

    final List<ExtendedRecord> expected = createCollection(false, false, id + "_" + somethingCore);

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

  private List<ExtendedRecord> createCollection(
      boolean isExt, boolean isHashedId, String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");

              Map<String, String> ext = new HashMap<>(2);
              ext.put(DwcTerm.occurrenceID.qualifiedName(), array[0]);
              ext.put("somethingCore", array[1]);
              if (array.length == 3) {
                ext.put("somethingExt", array[2]);
              }

              ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setCoreTerms(ext);

              if (isHashedId) {
                builder.setId(HashUtils.getSha1(datasetId, array[0]));
              } else {
                builder.setId(array[0]);
              }

              if (isExt) {
                builder.setExtensions(
                    Collections.singletonMap(Occurrence.qualifiedName(), Collections.emptyList()));
              }

              return builder.build();
            })
        .collect(Collectors.toList());
  }
}
