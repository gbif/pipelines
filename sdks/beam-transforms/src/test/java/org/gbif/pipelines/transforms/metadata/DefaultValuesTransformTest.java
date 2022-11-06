package org.gbif.pipelines.transforms.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class DefaultValuesTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void defaultOccurrenceStatusTest() {

    // State
    MachineTag machineTag = new MachineTag();
    machineTag.setName(DwcTerm.occurrenceStatus.name());
    machineTag.setNamespace(TagNamespace.GBIF_DEFAULT_TERM.getNamespace());
    machineTag.setValue(OccurrenceStatus.ABSENT.name());

    Dataset dataset = new Dataset();
    dataset.setMachineTags(Collections.singletonList(machineTag));

    List<ExtendedRecord> sourceList =
        Collections.singletonList(ExtendedRecord.newBuilder().setId("777").build());

    // Expected
    List<ExtendedRecord> expectedList =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(
                    Collections.singletonMap(
                        DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.ABSENT.name()))
                .build());

    // When
    PCollection<ExtendedRecord> resultCollection =
        p.apply(Create.of(sourceList))
            .apply(
                DefaultValuesTransform.builder()
                    .datasetId("")
                    .clientSupplier(() -> new MetadataServiceClientStub(dataset))
                    .create()
                    .interpret());

    // Should
    PAssert.that(resultCollection).containsInAnyOrder(expectedList);
    p.run();
  }

  @Test
  public void noTagsTest() {

    // State
    Dataset dataset = new Dataset();

    List<ExtendedRecord> sourceList =
        Collections.singletonList(ExtendedRecord.newBuilder().setId("777").build());

    // Expected
    List<ExtendedRecord> expectedList =
        Collections.singletonList(ExtendedRecord.newBuilder().setId("777").build());

    // When
    PCollection<ExtendedRecord> resultCollection =
        p.apply(Create.of(sourceList))
            .apply(
                DefaultValuesTransform.builder()
                    .datasetId("")
                    .clientSupplier(() -> new MetadataServiceClientStub(dataset))
                    .create()
                    .interpret());

    // Should
    PAssert.that(resultCollection).containsInAnyOrder(expectedList);
    p.run();
  }

  private static class MetadataServiceClientStub extends MetadataServiceClient
      implements Serializable {

    private final Dataset dataset;

    public MetadataServiceClientStub(Dataset dataset) {
      this.dataset = dataset;
    }

    @Override
    public Dataset getDataset(String datasetId) {
      return dataset;
    }
  }
}
