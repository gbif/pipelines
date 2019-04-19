package org.gbif.pipelines.transforms.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class OccurrenceExtensionTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void extensionContainsOccurrenceTest() {

    // State
    String id = "1";
    String somethingCore = "somethingCore";
    String somethingExt = "somethingExt";

    Map<String, String> ext1 = new HashMap<>(2);
    ext1.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext1.put(somethingExt, somethingExt);

    Map<String, String> ext2 = new HashMap<>(2);
    ext2.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext2.put(somethingExt, somethingExt);

    Map<String, String> ext3 = new HashMap<>(2);
    ext3.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext3.put(somethingExt, somethingExt);

    ExtendedRecord er = ExtendedRecord.newBuilder()
        .setId(id)
        .setCoreTerms(Collections.singletonMap(somethingCore, somethingCore))
        .setExtensions(Collections.singletonMap(DwcTerm.Occurrence.qualifiedName(), Arrays.asList(ext1, ext2, ext3)))
        .build();

    final List<ExtendedRecord> expected =
        createCollection(id + "_" + somethingCore + "_" + somethingExt, id + "_" + somethingCore + "_" + somethingExt,
            id + "_" + somethingCore + "_" + somethingExt);

    // When
    PCollection<ExtendedRecord> result = p.apply(Create.of(er)).apply(OccurrenceExtensionTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  private List<ExtendedRecord> createCollection(String... idName) {
    return Arrays.stream(idName)
        .map(x -> {
          String[] array = x.split("_");

          Map<String, String> ext = new HashMap<>(2);
          ext.put(DwcTerm.occurrenceID.qualifiedName(), array[0]);
          ext.put("somethingCore", array[1]);
          ext.put("somethingExt", array[2]);

          return ExtendedRecord.newBuilder()
              .setId(array[0])
              .setCoreTerms(ext)
              .build();
        })
        .collect(Collectors.toList());
  }

}
