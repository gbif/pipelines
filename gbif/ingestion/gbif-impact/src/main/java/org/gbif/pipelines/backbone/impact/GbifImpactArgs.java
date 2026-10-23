package org.gbif.pipelines.backbone.impact;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.Serializable;

/** Command line arguments for the GBIF impact Spark job. */
@Parameters(separators = "=")
public class GbifImpactArgs implements Serializable {

  @Parameter(
      names = "--sourcePath",
      description =
          "Path to a Parquet directory with classifications (see project readme). "
              + "When set, the Hive database/table are not used")
  public String sourcePath;

  @Parameter(names = "--database", description = "Hive database")
  public String database;

  @Parameter(
      names = "--table",
      description = "Source Hive table with classifications (see project readme)")
  public String table;

  @Parameter(
      names = "--metastoreUris",
      description = "Uri to hive Metastore, e.g.: thrift://hivesever2:9083")
  public String metastoreUris;

  @Parameter(
      names = "--targetDir",
      description = "Target directory to write the output to",
      required = true)
  public String targetDir;

  @Parameter(
      names = "--APIBaseURI",
      description = "Base URL for the API, e.g. https://api.gbif-uat.org/v1/",
      required = true)
  public String apiBaseUri;

  @Parameter(names = "--reportFileName", description = "The file name for final report")
  public String reportFileName = "report.csv";

  @Parameter(names = "--checklistKey", description = "The checklist key to use")
  public String checklistKey;

  @Parameter(
      names = "--scope",
      description = "A taxon key to limit to using the existing GBIF.org keys (e.g. 1 for Animals)")
  public Integer scope;

  @Parameter(
      names = "--minimumOccurrenceCount",
      description = "Minimum occurrenceCount to apply when filtering")
  public int minimumOccurrenceCount = 1;

  @Parameter(
      names = "--skipKeys",
      description = "Controls if keys should be omitted or not",
      arity = 1)
  public boolean skipKeys = false;

  @Parameter(
      names = "--ignoreWhitespace",
      description = "Controls if whitespace should be ignored or not",
      arity = 1)
  public boolean ignoreWhitespace = true;

  @Parameter(
      names = "--ignoreAuthorshipFormatting",
      description = "Controls if authorship formatting should be ignored or not",
      arity = 1)
  public boolean ignoreAuthorshipFormatting = true;

  @Parameter(
      names = "--numPartitions",
      description =
          "Repartition the source into this many partitions before the lookups to control "
              + "the parallelism against the API (0 keeps the source partitioning)")
  public int numPartitions = 0;

  @Parameter(names = "--hdfsSiteConfig", description = "Path to hdfs-site.xml")
  public String hdfsSiteConfig;

  @Parameter(names = "--coreSiteConfig", description = "Path to core-site.xml")
  public String coreSiteConfig;

  @Parameter(names = "--master", description = "Spark master, e.g. local[*] for local runs")
  public String master;

  @Parameter(names = "--appName", description = "Spark application name")
  public String appName = "gbif-impact";

  @Parameter(names = "--help", description = "Display help information", help = true)
  public boolean help = false;
}
