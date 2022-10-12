package org.gbif.pipelines.ingest.pipelines.interpretation;

import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.ClusteringServiceFactory;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.factory.GeocodeKvStoreFactory;
import org.gbif.pipelines.factory.GrscicollLookupKvStoreFactory;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.common.FilterRecordsTransform;
import org.gbif.pipelines.transforms.common.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.common.UniqueIdTransform;
import org.gbif.pipelines.transforms.converters.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.DefaultValuesTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform.GbifIdTransformBuilder;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.species.NameUsageMatch;

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

  public MetadataTransform createMetadataTransform() {
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSupplier = null;
    if (options.getUseMetadataWsCalls() && !options.getTestMode()) {
      metadataServiceClientSupplier = MetadataServiceClientFactory.createSupplier(config);
    }
    return MetadataTransform.builder()
        .clientSupplier(metadataServiceClientSupplier)
        .attempt(options.getAttempt())
        .endpointType(options.getEndPointType())
        .create();
  }

  public GbifIdAbsentTransform createGbifIdAbsentTransform() {
    SerializableSupplier<HBaseLockingKey> keyServiceSupplier = null;
    if (!options.isUseExtendedRecordId()) {
      keyServiceSupplier = KeygenServiceFactory.createSupplier(config, options.getDatasetId());
    }
    return GbifIdAbsentTransform.builder()
        .isTripletValid(options.isTripletValid())
        .isOccurrenceIdValid(options.isOccurrenceIdValid())
        .keygenServiceSupplier(keyServiceSupplier)
        .create();
  }

  public BasicTransform createBasicTransform() {
    return BasicTransform.builder()
        .useDynamicPropertiesInterpretation(true)
        .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.createSupplier(config))
        .vocabularyServiceSupplier(
            FileVocabularyFactory.builder()
                .config(config)
                .hdfsConfigs(hdfsConfigs)
                .build()
                .getInstanceSupplier())
        .create();
  }

  public ClusteringTransform createClusteringTransform() {
    return ClusteringTransform.builder()
        .clusteringServiceSupplier(ClusteringServiceFactory.createSupplier(config))
        .create();
  }

  public VerbatimTransform createVerbatimTransform() {
    return VerbatimTransform.create();
  }

  public GbifIdTransform createGbifIdTransform() {
    GbifIdTransformBuilder gbifIdTransformBuilder = GbifIdTransform.builder();
    if (useGbifIdRecordWriteIO(options.getInterpretationTypes())) {
      SerializableSupplier<HBaseLockingKey> keyServiceSupplier = null;
      if (!options.isUseExtendedRecordId()) {
        keyServiceSupplier = KeygenServiceFactory.createSupplier(config, options.getDatasetId());
      }
      gbifIdTransformBuilder
          .isTripletValid(options.isTripletValid())
          .isOccurrenceIdValid(options.isOccurrenceIdValid())
          .useExtendedRecordId(options.isUseExtendedRecordId())
          .generateIdIfAbsent(true)
          .keygenServiceSupplier(keyServiceSupplier);
    }
    return gbifIdTransformBuilder.create();
  }

  public TemporalTransform createTemporalTransform() {
    return TemporalTransform.builder().orderings(dateComponentOrdering).create();
  }

  public TaxonomyTransform createTaxonomyTransform() {
    SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
        nameUsageMatchServiceSupplier = null;
    if (!options.getTestMode()) {
      nameUsageMatchServiceSupplier = NameUsageMatchStoreFactory.createSupplier(config);
    }
    return TaxonomyTransform.builder().kvStoreSupplier(nameUsageMatchServiceSupplier).create();
  }

  public GrscicollTransform createGrscicollTransform() {
    SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
        grscicollServiceSupplier = null;
    if (!options.getTestMode()) {
      grscicollServiceSupplier = GrscicollLookupKvStoreFactory.createSupplier(config);
    }
    return GrscicollTransform.builder().kvStoreSupplier(grscicollServiceSupplier).create();
  }

  public LocationTransform createLocationTransform() {
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeServiceSupplier = null;
    if (!options.getTestMode()) {
      geocodeServiceSupplier = GeocodeKvStoreFactory.createSupplier(hdfsConfigs, config);
    }
    return LocationTransform.builder().geocodeKvStoreSupplier(geocodeServiceSupplier).create();
  }

  public EventCoreTransform createEventCoreTransform() {
    return EventCoreTransform.builder()
        .vocabularyServiceSupplier(
            FileVocabularyFactory.builder()
                .config(config)
                .hdfsConfigs(hdfsConfigs)
                .build()
                .getInstanceSupplier())
        .create();
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

  public UniqueGbifIdTransform createUniqueGbifIdTransform() {
    return UniqueGbifIdTransform.create(options.isUseExtendedRecordId());
  }

  public UniqueIdTransform createUniqueIdTransform() {
    return UniqueIdTransform.create();
  }

  public SingleOutput<ExtendedRecord, ExtendedRecord> createOccurrenceExtensionTransform() {
    return OccurrenceExtensionTransform.create();
  }

  public SingleOutput<KV<String, CoGbkResult>, ExtendedRecord> createFilterRecordsTransform(
      VerbatimTransform verbatimTransform, GbifIdTransform idTransform) {
    return FilterRecordsTransform.create(verbatimTransform.getTag(), idTransform.getTag()).filter();
  }

  public MeasurementOrFactTransform createMeasurementOrFactTransform() {
    return MeasurementOrFactTransform.builder().create();
  }

  private static boolean useGbifIdRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.IDENTIFIER.name()) || types.contains(RecordType.ALL.name());
  }
}
