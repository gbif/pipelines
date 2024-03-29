package au.org.ala.pipelines.transforms;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import com.google.common.base.Strings;
import java.util.*;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;

/**
 * Most of this class if copied from GBIF' {@link DefaultValuesTransform}. The only difference is
 * where the default values are sourced from which. In GBIF's case this is the GBIF Registry, for
 * ALA it is the registry
 *
 * <p>TODO Discuss with GBIF how we can make {@link DefaultValuesTransform} class extensible.
 */
@Slf4j
public class ALADefaultValuesTransform
    extends PTransform<PCollection<ExtendedRecord>, PCollection<ExtendedRecord>> {

  private static final String DEFAULT_TERM_NAMESPACE =
      TagNamespace.GBIF_DEFAULT_TERM.getNamespace();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceKvStoreSupplier;

  private final String datasetId;

  @Builder(buildMethodName = "create")
  private ALADefaultValuesTransform(
      String datasetId,
      KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore,
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceKvStoreSupplier) {
    this.datasetId = datasetId;
    this.dataResourceKvStoreSupplier = dataResourceKvStoreSupplier;
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
    try {
      List<MachineTag> tags = new ArrayList<>();
      ALACollectoryMetadata metadata = dataResourceKvStoreSupplier.get().get(datasetId);
      if (metadata != null
          && metadata.getDefaultDarwinCoreValues() != null
          && !metadata.getDefaultDarwinCoreValues().isEmpty()) {
        for (Map.Entry<String, String> entry : metadata.getDefaultDarwinCoreValues().entrySet()) {
          tags.add(
              MachineTag.newInstance(DEFAULT_TERM_NAMESPACE, entry.getKey(), entry.getValue()));
        }
      }
      return tags;
    } catch (Exception e) {
      log.error("Problem retrieving collectory data: " + e.getMessage(), e);
      throw new PipelinesException(e.getMessage());
    }
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

  public void replaceDefaultValues(Map<String, ExtendedRecord> source) {
    List<MachineTag> tags = getMachineTags();
    if (!tags.isEmpty()) {
      source.forEach((key, value) -> source.put(key, replaceDefaultValues(value, tags)));
    }
  }
}
