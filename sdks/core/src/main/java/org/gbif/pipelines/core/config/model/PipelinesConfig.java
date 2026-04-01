package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.common.parsers.date.DateComponentOrdering;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelinesConfig implements Serializable {

  private static final long serialVersionUID = 8102560635064341713L;

  /** The directory where the input files are located */
  private String inputPath;

  /** The directory where the output files are written to */
  private String outputPath;

  /**
   * The directory where the intermediate files & debug outputs are written to as part of the
   * rebuild jobs
   */
  private String rebuildPath;

  private String hdfsSiteConfig = "/etc/hadoop/conf/hdfs-site.xml";

  private String coreSiteConfig = "/etc/hadoop/conf/core-site.xml";

  private String hbaseSiteConfig = "/etc/hadoop/conf/hbase-site.xml";

  private String hiveDB;

  private String hdfsWarehousePath;

  private String hiveMetastoreUris;

  private String zkConnectionString;

  private WsConfig gbifApi;

  private String imageCachePath = "bitmap/bitmap.png";

  private KvConfig nameUsageMatch;

  private ChecklistKvConfig nameUsageMatchingService;

  private KvConfig grscicollLookup;

  private KvConfig geocode;

  private KvConfig locationFeature;

  private ContentConfig content;

  private WsConfig amplification;

  private KeygenConfig keygen;

  private LockConfig indexLock;

  private LockConfig hdfsLock;

  private EsConfig elastic;

  private VocabularyConfig vocabularyConfig;

  private WsConfig vocabularyService;

  private ClusteringRelationshipConfig clusteringRelationshipConfig;

  /**
   * Provide recommended formats to parse ambiguous dates, e.g. 2/3/2008. If the field is empty or
   * invalid, only accepts standard ISO date format. Parsing 2/3/2008 will fail . <code>DMY</code>
   * will parse 2/3/2008 as 2 Mar 2008 <code>MDY</code> will parse 2/3/2008 as 3 Feb 2008
   */
  private List<DateComponentOrdering> defaultDateFormat =
      Arrays.asList(DateComponentOrdering.ISO_FORMATS);

  private Set<String> extensionsAllowedForVerbatimSet;

  private String fragmentsTable;

  private String fragmentHfilePath = "fragment";

  private String fragmentHfileStagingPath = "hfile-staging";

  private StandaloneConfig standalone;

  private AirflowConfig airflowConfig;

  private Map<String, SparkJobConfig> processingConfigs;

  private IndexConfig indexConfig;

  private DerivedMetadataConfig derivedMetadataConfig;

  private TableBuildConfig tableBuildConfig;
}
