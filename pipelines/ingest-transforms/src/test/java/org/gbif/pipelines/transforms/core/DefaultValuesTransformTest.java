package org.gbif.pipelines.transforms.core;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.core.MetadataInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.Collections;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class DefaultValuesTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void defaultValueUseTest() {
    //Expected
    String expectedFamily = "F";
    ExtendedRecord expected = ExtendedRecord.newBuilder().setId("777").setCoreTerms(Collections.singletonMap(DwcTerm.family.simpleName(), expectedFamily)).build();

    // State
    MetadataRecord mdr = MetadataRecord.newBuilder().setId("777")
      .setMachineTags(Collections.singletonList(MachineTag.newBuilder()
                                                  .setNamespace(MetadataInterpreter.DEFAULT_TERM_NAMESPACE)
                                                  .setName(DwcTerm.family.simpleName())
                                                  .setValue(expectedFamily).build())).build();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(Collections.singletonMap(DwcTerm.family.simpleName(), "G")).build();

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata",Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    // When
    PCollection<ExtendedRecord> recordCollection =
        p.apply(Create.of(er)).apply(DefaultValuesTransform.create().interpret(metadataView));

    // Should: default value be there
    PAssert.that(recordCollection).containsInAnyOrder(expected);

    // Should: default value was replaced
    PAssert.that(recordCollection).satisfies(records -> {
      Assert.assertTrue(StreamSupport.stream(records.spliterator(), false).noneMatch(record -> "G".equals(record.getCoreTerms().get(DwcTerm.family.simpleName()))));
      return null;
    });

    p.run();
  }
}
