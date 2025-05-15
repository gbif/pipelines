package org.gbif.pipelines.backbone.impact;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Pipeline settings and arguments for Hbase to Avro export. */
public interface BackbonePreReleaseOptions extends HadoopFileSystemOptions {

  @Description("Hive database")
  String getDatabase();

  void setDatabase(String database);

  @Description("Source table with classifications (see project readme)")
  String getTable();

  void setTable(String table);

  @Description("Target directory to write the output to")
  String getTargetDir();

  void setTargetDir(String targetDir);

  @Description("Uri to hive Metastore, e.g.: thrift://hivesever2:9083")
  String getMetastoreUris();

  void setMetastoreUris(String metastoreUris);

  @Description("Base URL for the API, e.g. https://api.gbif-uat.org/v1/")
  String getAPIBaseURI();

  void setAPIBaseURI(String baseUri);

  @Description("The file name for final report")
  @Default.String("report.csv")
  String getReportFileName();

  void setReportFileName(String reportFileName);

  @Description("The checklist key to use")
  String getChecklistKey();

  void setChecklistKey(String checklistKey);

  @Description("A taxon key to limit to using the existing GBIF.org keys (e.g. 1 for Animals")
  Integer getScope();

  void setScope(Integer scope);

  @Description("Minimum occurrenceCount to apply when filtering")
  @Default.Integer(1)
  int getMinimumOccurrenceCount();

  void setMinimumOccurrenceCount(int minimumOccurrenceCount);

  @Description("Controls if keys should be omitted or not")
  @Default.Boolean(false)
  boolean getSkipKeys();

  void setSkipKeys(boolean skipKeys);

  @Description("Controls if whitespace should be ignored or not")
  @Default.Boolean(true)
  boolean getIgnoreWhitespace();

  void setIgnoreWhitespace(boolean ignoreWhitespace);

  @Description("Controls if whitespace should be ignored or not")
  @Default.Boolean(true)
  boolean getIgnoreAuthorshipFormatting();

  void setIgnoreAuthorshipFormatting(boolean ignoreAuthorshipFormatting);

  @Description("Path to hdfs-site-config.xml")
  String getHdfsSiteConfig();

  void setHdfsSiteConfig(String path);

  @Description("Path to core-site-config.xml")
  String getCoreSiteConfig();

  void setCoreSiteConfig(String path);

  @Description(
      "Output infrageneric markers in the scientific name (CLB v2 includes these in the scientific name)")
  @Default.Boolean(false)
  boolean getOutputInfragenericEpithet();

  void setOutputInfragenericEpithet(boolean outputInfragenericEpithet);

  @Description("Whether to include the rank in the CLB lookup (CLB matching only)")
  @Default.Boolean(true)
  boolean getIgnoreSuppliedRank();

  void setIgnoreSuppliedRank(boolean ignoreSuppliedRank);
}
