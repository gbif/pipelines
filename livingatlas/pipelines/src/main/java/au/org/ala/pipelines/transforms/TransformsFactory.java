package au.org.ala.pipelines.transforms;

import java.util.List;
import lombok.Getter;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;

/** Factory class for initialising transforms. */
@Getter
public class TransformsFactory {

  private final InterpretationPipelineOptions options;
  private final HdfsConfigs hdfsConfigs;
  private final PipelinesConfig config;
  private final List<DateComponentOrdering> dateComponentOrdering;

  private TransformsFactory(InterpretationPipelineOptions options) {
    this.options = options;
    this.hdfsConfigs = HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    this.config =
        FsUtils.readConfigFile(hdfsConfigs, options.getProperties(), PipelinesConfig.class);
    this.dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getDefaultDateFormat()
            : options.getDefaultDateFormat();
  }

  public static TransformsFactory create(InterpretationPipelineOptions options) {
    return new TransformsFactory(options);
  }

  public SingleOutput<ExtendedRecord, ExtendedRecord> createDefaultValuesTransform() {
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSupplier = null;
    if (options.getUseMetadataWsCalls() && !options.getTestMode()) {
      metadataServiceClientSupplier = MetadataServiceClientFactory.createSupplier(config);
    }
    return DefaultValuesTransform.builder()
        .clientSupplier(metadataServiceClientSupplier)
        .datasetId(options.getDatasetId())
        .create()
        .interpret();
  }

  public ExtensionFilterTransform createExtensionFilterTransform() {
    return ExtensionFilterTransform.create(config.getExtensionsAllowedForVerbatimSet());
  }

  public IdentifierTransform createIdentifierTransform() {
    return IdentifierTransform.builder().datasetKey(options.getDatasetId()).create();
  }

  public MultimediaTransform createMultimediaTransform() {
    return MultimediaTransform.builder().orderings(dateComponentOrdering).create();
  }

  public AudubonTransform createAudubonTransform() {
    return AudubonTransform.builder().orderings(dateComponentOrdering).create();
  }

  public ImageTransform createImageTransform() {
    return ImageTransform.builder().orderings(dateComponentOrdering).create();
  }

  public UniqueIdTransform createUniqueIdTransform() {
    return UniqueIdTransform.create();
  }
}
