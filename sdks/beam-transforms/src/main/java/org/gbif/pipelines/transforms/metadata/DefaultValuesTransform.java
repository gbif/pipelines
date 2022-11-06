package org.gbif.pipelines.transforms.metadata;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DEFAULT_VALUES_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.VERBATIM;

import com.google.common.base.Strings;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations to use verbatim default term values defined as MachineTags in an
 * MetadataRecord. transforms form {@link ExtendedRecord} to {@link ExtendedRecord}.
 */
@Slf4j
public class DefaultValuesTransform extends Transform<ExtendedRecord, ExtendedRecord> {

  private static final String DEFAULT_TERM_NAMESPACE =
      TagNamespace.GBIF_DEFAULT_TERM.getNamespace();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private final String datasetId;
  private final SerializableSupplier<MetadataServiceClient> clientSupplier;

  @Getter private List<MachineTag> tags;

  @Builder(buildMethodName = "create")
  private DefaultValuesTransform(
      String datasetId, SerializableSupplier<MetadataServiceClient> clientSupplier) {
    super(
        ExtendedRecord.class,
        VERBATIM,
        ExtendedRecord.class.getName(),
        DEFAULT_VALUES_RECORDS_COUNT);
    this.datasetId = datasetId;
    this.clientSupplier = clientSupplier;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    MetadataServiceClient client = null;
    if (clientSupplier != null) {
      log.info("Initialize MetadataServiceClient");
      client = clientSupplier.get();
    }
    if (client != null && tags == null) {
      Dataset dataset = client.getDataset(datasetId);
      tags = getMachineTags(dataset);
    }
    if (tags == null) {
      log.error("MachineTags list is null, datasetKey - {}", datasetId);
      tags = Collections.emptyList();
    }
  }

  @Override
  public Optional<ExtendedRecord> convert(ExtendedRecord source) {
    if (tags.isEmpty()) {
      return Optional.of(source);
    }

    ExtendedRecord erWithDefault = ExtendedRecord.newBuilder(source).build();

    tags.forEach(
        tag -> {
          Term term = TERM_FACTORY.findPropertyTerm(tag.getName());
          String defaultValue = tag.getValue();
          if (term != null && !Strings.isNullOrEmpty(defaultValue)) {
            erWithDefault.getCoreTerms().putIfAbsent(term.qualifiedName(), tag.getValue());
          }
        });

    return Optional.of(erWithDefault);
  }

  private List<MachineTag> getMachineTags(Dataset dataset) {
    List<MachineTag> mt = Collections.emptyList();
    if (dataset != null
        && dataset.getMachineTags() != null
        && !dataset.getMachineTags().isEmpty()) {
      mt =
          dataset.getMachineTags().stream()
              .filter(tag -> DEFAULT_TERM_NAMESPACE.equalsIgnoreCase(tag.getNamespace()))
              .collect(Collectors.toList());
    }
    return mt;
  }
}
