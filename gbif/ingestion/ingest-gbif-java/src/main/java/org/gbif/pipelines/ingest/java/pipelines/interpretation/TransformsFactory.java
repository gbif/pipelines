package org.gbif.pipelines.ingest.java.pipelines.interpretation;

import java.util.List;
import lombok.Getter;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.grscicoll.GrscicollLookupRequest;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.common.beam.metrics.IngestMetrics;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.factory.ConfigFactory;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.factory.ClusteringServiceFactory;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.factory.GeocodeKvStoreFactory;
import org.gbif.pipelines.factory.GrscicollLookupKvStoreFactory;
import org.gbif.pipelines.factory.KeygenServiceFactory;
import org.gbif.pipelines.factory.MetadataServiceClientFactory;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.ingest.java.metrics.IngestMetricsBuilder;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.java.DefaultValuesTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;
import org.gbif.rest.client.species.NameUsageMatch;

public class TransformsFactory {

  @Getter
  private final IngestMetrics metrics = IngestMetricsBuilder.createVerbatimToInterpretedMetrics();

  @Getter private final SerializableConsumer<String> incMetricFn = metrics::incMetric;
  private final InterpretationPipelineOptions options;
  private final HdfsConfigs hdfsConfigs;
  private final PipelinesConfig config;
  private final List<DateComponentOrdering> dateComponentOrdering;

  private TransformsFactory(InterpretationPipelineOptions options) {
    this.options = options;
    this.hdfsConfigs = HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());
    this.config =
        ConfigFactory.getInstance(hdfsConfigs, options.getProperties(), PipelinesConfig.class)
            .get();
    this.dateComponentOrdering =
        options.getDefaultDateFormat() == null
            ? config.getDefaultDateFormat()
            : options.getDefaultDateFormat();
  }

  public static TransformsFactory create(InterpretationPipelineOptions options) {
    return new TransformsFactory(options);
  }

  public MetadataTransform createMetadataTransform() {
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSupplier = null;
    if (options.getUseMetadataWsCalls() && !options.getTestMode()) {
      metadataServiceClientSupplier = MetadataServiceClientFactory.getInstanceSupplier(config);
    }
    return MetadataTransform.builder()
        .clientSupplier(metadataServiceClientSupplier)
        .attempt(options.getAttempt())
        .endpointType(options.getEndPointType())
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public GbifIdAbsentTransform createGbifIdAbsentTransform() {
    SerializableSupplier<HBaseLockingKey> keyServiceSupplier = null;
    if (!options.isUseExtendedRecordId()) {
      keyServiceSupplier = KeygenServiceFactory.getInstanceSupplier(config, options.getDatasetId());
    }
    return GbifIdAbsentTransform.builder()
        .isTripletValid(options.isTripletValid())
        .isOccurrenceIdValid(options.isOccurrenceIdValid())
        .keygenServiceSupplier(keyServiceSupplier)
        .create()
        .init();
  }

  public GbifIdTransform createGbifIdTransform() {
    SerializableSupplier<HBaseLockingKey> keyServiceSupplier = null;
    if (!options.isUseExtendedRecordId()) {
      keyServiceSupplier = KeygenServiceFactory.getInstanceSupplier(config, options.getDatasetId());
    }
    return GbifIdTransform.builder()
        .isTripletValid(options.isTripletValid())
        .isOccurrenceIdValid(options.isOccurrenceIdValid())
        .useExtendedRecordId(options.isUseExtendedRecordId())
        .keygenServiceSupplier(keyServiceSupplier)
        .generateIdIfAbsent(options.getGenerateIds())
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public ClusteringTransform createClusteringTransform() {
    return ClusteringTransform.builder()
        .clusteringServiceSupplier(ClusteringServiceFactory.getInstanceSupplier(config))
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public BasicTransform createBasicTransform() {
    return BasicTransform.builder()
        .useDynamicPropertiesInterpretation(true)
        .occStatusKvStoreSupplier(OccurrenceStatusKvStoreFactory.getInstanceSupplier(config))
        .vocabularyServiceSupplier(
            FileVocabularyFactory.builder()
                .config(config)
                .hdfsConfigs(hdfsConfigs)
                .build()
                .getInstanceSupplier())
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public TaxonomyTransform createTaxonomyTransform() {
    SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
        nameUsageMatchServiceSupplier = null;
    if (!options.getTestMode()) {
      nameUsageMatchServiceSupplier = NameUsageMatchStoreFactory.getInstanceSupplier(config);
    }
    return TaxonomyTransform.builder()
        .kvStoreSupplier(nameUsageMatchServiceSupplier)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public VerbatimTransform createVerbatimTransform() {
    return VerbatimTransform.create().counterFn(incMetricFn);
  }

  public GrscicollTransform createGrscicollTransform() {
    SerializableSupplier<KeyValueStore<GrscicollLookupRequest, GrscicollLookupResponse>>
        grscicollServiceSupplier = null;
    if (!options.getTestMode()) {
      grscicollServiceSupplier = GrscicollLookupKvStoreFactory.getInstanceSupplier(config);
    }
    return GrscicollTransform.builder()
        .kvStoreSupplier(grscicollServiceSupplier)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public LocationTransform createLocationTransform() {
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeServiceSupplier = null;
    if (!options.getTestMode()) {
      geocodeServiceSupplier = GeocodeKvStoreFactory.getInstanceSupplier(hdfsConfigs, config);
    }
    return LocationTransform.builder()
        .geocodeKvStoreSupplier(geocodeServiceSupplier)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public TemporalTransform createTemporalTransform() {
    return TemporalTransform.builder()
        .orderings(dateComponentOrdering)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public MultimediaTransform createMultimediaTransform() {
    return MultimediaTransform.builder()
        .orderings(dateComponentOrdering)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public AudubonTransform createAudubonTransform() {
    return AudubonTransform.builder()
        .orderings(dateComponentOrdering)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public ImageTransform createImageTransform() {
    return ImageTransform.builder()
        .orderings(dateComponentOrdering)
        .create()
        .counterFn(incMetricFn)
        .init();
  }

  public OccurrenceExtensionTransform createOccurrenceExtensionTransform() {
    return OccurrenceExtensionTransform.create().counterFn(incMetricFn);
  }

  public ExtensionFilterTransform createExtensionFilterTransform() {
    return ExtensionFilterTransform.create(config.getExtensionsAllowedForVerbatimSet());
  }

  public DefaultValuesTransform createDefaultValuesTransform() {
    SerializableSupplier<MetadataServiceClient> metadataServiceClientSupplier = null;
    if (options.getUseMetadataWsCalls() && !options.getTestMode()) {
      metadataServiceClientSupplier = MetadataServiceClientFactory.getInstanceSupplier(config);
    }
    return DefaultValuesTransform.builder()
        .clientSupplier(metadataServiceClientSupplier)
        .datasetId(options.getDatasetId())
        .create()
        .init();
  }
}
