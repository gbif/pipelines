package org.gbif.pipelines.transforms.metadata;

import com.google.common.base.Strings;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.SerializableSupplier;

/**
 * Beam level transformations to use verbatim default term values defined as MachineTags in an
 * MetadataRecord. transforms form {@link ExtendedRecord} to {@link ExtendedRecord}.
 */
@Slf4j
@Builder(buildMethodName = "create")
public class DefaultValuesTransform
    extends PTransform<PCollection<ExtendedRecord>, PCollection<ExtendedRecord>> {

  private static final String DEFAULT_TERM_NAMESPACE =
      TagNamespace.GBIF_DEFAULT_TERM.getNamespace();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private final String datasetId;
  private SerializableSupplier<MetadataServiceClient> clientSupplier;
  private MetadataServiceClient client;

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (client == null && clientSupplier != null) {
      log.info("Initialize MetadataServiceClient");
      client = clientSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (client != null) {
      log.info("Close MetadataServiceClient");
      client.close();
    }
  }

  /**
   * If the condition is FALSE returns empty collections, if you will you "write" data, it will
   * create an empty file, which is useful when you "read" files, cause Beam can throw an exception
   * if a file is absent
   */
  @Override
  public PCollection<ExtendedRecord> expand(PCollection<ExtendedRecord> input) {
    List<MachineTag> tags = getMachineTags();
    return tags.isEmpty() ? input : ParDo.of(createDoFn(tags)).expand(input);
  }

  private DoFn<ExtendedRecord, ExtendedRecord> createDoFn(List<MachineTag> tags) {
    return new DoFn<ExtendedRecord, ExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(replaceDefaultValues(c.element(), tags));
      }
    };
  }

  public List<MachineTag> getMachineTags() {
    List<MachineTag> tags = Collections.emptyList();
    if (client != null) {
      Dataset dataset = client.getDataset(datasetId);
      if (dataset != null
          && dataset.getMachineTags() != null
          && !dataset.getMachineTags().isEmpty()) {
        tags =
            dataset.getMachineTags().stream()
                .filter(tag -> DEFAULT_TERM_NAMESPACE.equalsIgnoreCase(tag.getNamespace()))
                .collect(Collectors.toList());
      }
    }
    return tags;
  }

  public ExtendedRecord replaceDefaultValues(ExtendedRecord er, List<MachineTag> tags) {
    ExtendedRecord erWithDefault = ExtendedRecord.newBuilder(er).build();

    tags.forEach(
        tag -> {
          Term term = TERM_FACTORY.findPropertyTerm(tag.getName());
          String defaultValue = tag.getValue();
          if (term != null && !Strings.isNullOrEmpty(defaultValue)) {
            erWithDefault.getCoreTerms().putIfAbsent(term.qualifiedName(), tag.getValue());
          }
        });

    return erWithDefault;
  }
}
