package org.gbif.pipelines.ingest.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.File;
import java.util.Set;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Pipeline options (configuration) for GBIF based data interpretation pipelines. Optionally can use
 * a {@link HadoopFileSystemOptions} when exporting/reading files.
 */
public interface InterpretationPipelineOptions
    extends BasePipelineOptions, HadoopFileSystemOptions, SparkPipelineOptions {

  @Override
  @Description(
      "Default directory where the target file will be written. By default, it takes the hdfs root directory "
          + "specified in \"fs.defaultFS\". If no configurations are set it takes \"hdfs://\" as default")
  @Default.InstanceFactory(DefaultDirectoryFactory.class)
  String getTargetPath();

  @Override
  void setTargetPath(String targetPath);

  @Description(
      "A HDFS default location for storing temporary files. "
          + "By default uses a tmp directory in the root folder")
  @Default.InstanceFactory(TempDirectoryFactory.class)
  String getHdfsTempLocation();

  void setHdfsTempLocation(String value);

  @Description("Types for an interpretation - ALL, TAXON, LOCATION and etc.")
  Set<String> getInterpretationTypes();

  void setInterpretationTypes(Set<String> types);

  @Description("Properties for interpretations that require the use of external web services")
  @JsonIgnore
  String getProperties();

  void setProperties(String path);

  @Description("Path to hdfs-site-config.xml")
  String getHdfsSiteConfig();

  void setHdfsSiteConfig(String path);

  @Description("Path to core-site-config.xml")
  String getCoreSiteConfig();

  void setCoreSiteConfig(String path);

  @Description("Type of the endpoint being crawled")
  @Default.String("DWC_ARCHIVE")
  String getEndPointType();

  void setEndPointType(String id);

  @Description("DWCA validation from crawler, all triplets are unique")
  @Default.Boolean(true)
  boolean isTripletValid();

  void setTripletValid(boolean tripletValid);

  @Description("DWCA validation from crawler, all occurrenceIds are unique")
  @Default.Boolean(true)
  boolean isOccurrenceIdValid();

  void setOccurrenceIdValid(boolean occurrenceIdValid);

  @Description("Skips gbif id generation and copies ids from ExtendedRecord ids")
  @Default.Boolean(false)
  boolean isUseExtendedRecordId();

  void setUseExtendedRecordId(boolean useExtendedRecordId);

  @Description("Number of file shards")
  Integer getNumberOfShards();

  void setNumberOfShards(Integer numberOfShards);

  /** A {@link DefaultValueFactory} which locates a default directory. */
  class TempDirectoryFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      return DefaultDirectoryFactory.getDefaultFs(options)
          .map(fs -> fs + File.separator + "tmp")
          .orElse("hdfs://tmp"); // in case no configurations are provided
    }
  }
}
