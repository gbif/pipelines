package org.gbif.pipelines.core.factory;

import com.google.common.base.Strings;
import java.io.File;
import java.net.URI;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@SuppressWarnings("all")
public class FileSystemFactory {

  private static volatile FileSystemFactory instance;

  private static final String DEFAULT_FS = "file:///";

  private final FileSystem localFs;
  private final FileSystem hdfsFs;

  private final String hdfsPrefix;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private FileSystemFactory(HdfsConfigs hdfsConfigs) {
    if (!Strings.isNullOrEmpty(hdfsConfigs.getHdfsSiteConfig())) {

      String hdfsPrefixToUse = getHdfsPrefix(hdfsConfigs);
      String corePrefixToUse = getHdfsPrefix(hdfsConfigs);

      String prefixToUse = null;
      if (!DEFAULT_FS.equals(hdfsPrefixToUse)) {
        prefixToUse = hdfsPrefixToUse;
      } else if (!DEFAULT_FS.equals(corePrefixToUse)) {
        prefixToUse = corePrefixToUse;
      } else {
        prefixToUse = hdfsPrefixToUse;
      }

      if (prefixToUse != null) {
        this.hdfsPrefix = prefixToUse;
        Configuration config = getHdfsConfiguration(hdfsConfigs);
        this.hdfsFs = FileSystem.get(URI.create(prefixToUse), config);
      } else {
        throw new PipelinesException("XML config is provided, but fs name is not found");
      }

    } else {
      this.hdfsPrefix = null;
      this.hdfsFs = null;
    }

    this.localFs = FileSystem.getLocal(new Configuration());
  }

  public static FileSystemFactory getInstance(HdfsConfigs hdfsConfigs) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new FileSystemFactory(hdfsConfigs);
        }
      }
    }
    return instance;
  }

  public static FileSystemFactory create(HdfsConfigs hdfsConfigs) {
    return new FileSystemFactory(hdfsConfigs);
  }

  public FileSystem getFs(String path) {
    if (path != null) {
      // using startsWith to allow for EMR style paths of hdfs:///
      if (hdfsPrefix != null && path.startsWith(hdfsPrefix)) {
        return hdfsFs;
      } else if (path.startsWith(FsUtils.HDFS_EMR_PREFIX)) {
        return hdfsFs;
      } else {
        return localFs;
      }
    } else {
      return localFs;
    }
  }

  public FileSystem getLocalFs() {
    return localFs;
  }

  public FileSystem getHdfsFs() {
    return hdfsFs;
  }

  /**
   * Creates an instances of a {@link Configuration} using a xml HDFS configuration file.
   *
   * @param pathToConfig coreSiteConfig path to the hdfs-site.xml or core-site.xml
   * @return a {@link Configuration} based on the provided config file
   */
  @SneakyThrows
  private static Configuration getHdfsConfiguration(HdfsConfigs hdfsConfigs) {
    // check if the hdfs-site.xml is provided
    if (!Strings.isNullOrEmpty(hdfsConfigs.getHdfsSiteConfig())
        && !Strings.isNullOrEmpty(hdfsConfigs.getCoreSiteConfig())) {
      File hdfsSiteFile = new File(hdfsConfigs.getHdfsSiteConfig());
      File coreSiteFile = new File(hdfsConfigs.getCoreSiteConfig());
      if (hdfsSiteFile.exists()
          && hdfsSiteFile.isFile()
          && coreSiteFile.exists()
          && coreSiteFile.isFile()) {
        Configuration config = new Configuration(false);
        log.info("Using XML config found at {} and {}", hdfsSiteFile, coreSiteFile);
        config.addResource(hdfsSiteFile.toURI().toURL());
        config.addResource(coreSiteFile.toURI().toURL());
        return config;
      } else {
        log.warn(
            "XML config does not exist - {} or {}",
            hdfsConfigs.getHdfsSiteConfig(),
            hdfsConfigs.getCoreSiteConfig());
      }
    } else {
      log.info("XML config not provided");
    }
    return new Configuration();
  }

  private static String getHdfsPrefix(HdfsConfigs hdfsConfigs) {
    String hdfsPrefixToUse = null;
    if (!Strings.isNullOrEmpty(hdfsConfigs.getHdfsSiteConfig())
        && !Strings.isNullOrEmpty(hdfsConfigs.getCoreSiteConfig())) {
      Configuration hdfsSite = getHdfsConfiguration(hdfsConfigs);
      hdfsPrefixToUse = hdfsSite.get("fs.default.name");
      if (hdfsPrefixToUse == null) {
        hdfsPrefixToUse = hdfsSite.get("fs.defaultFS");
      }
    }
    return hdfsPrefixToUse;
  }
}
