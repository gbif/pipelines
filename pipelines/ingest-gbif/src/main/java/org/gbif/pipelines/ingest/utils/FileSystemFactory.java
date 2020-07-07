package org.gbif.pipelines.ingest.utils;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Strings;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("all")
public class FileSystemFactory {

  private static volatile FileSystemFactory instance;

  private final FileSystem localFs;
  private final FileSystem hdfsFs;

  private final String hdfsPrefix;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private FileSystemFactory(String hdfsSiteConfig, String coreSiteConfig) {
    if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {

      String hdfsPrefixToUse = getHdfsPrefix(hdfsSiteConfig);
      if (hdfsPrefixToUse == null && !Strings.isNullOrEmpty(coreSiteConfig)) {
        hdfsPrefixToUse = getHdfsPrefix(coreSiteConfig);
      }

      if (hdfsPrefixToUse != null) {
        this.hdfsPrefix = hdfsPrefixToUse;
        Configuration config = getHdfsConfiguration(hdfsSiteConfig);
        this.hdfsFs = FileSystem.get(URI.create(hdfsPrefixToUse), config);
      } else {
        throw new RuntimeException("XML config is provided, but fs name is not found");
      }

    } else {
      this.hdfsPrefix = null;
      this.hdfsFs = null;
    }

    this.localFs = FileSystem.get(getHdfsConfiguration(null));
  }

  public static FileSystemFactory getInstance(String hdfsSiteConfig, String coreSiteConfig) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new FileSystemFactory(hdfsSiteConfig, coreSiteConfig);
        }
      }
    }
    return instance;
  }

  public static FileSystemFactory create(String hdfsSiteConfig, String coreSiteConfig) {
    return new FileSystemFactory(hdfsSiteConfig, coreSiteConfig);
  }

  public FileSystem getFs(String path) {
    return path != null && hdfsPrefix != null && path.startsWith(hdfsPrefix) ? hdfsFs : localFs;
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
  private static Configuration getHdfsConfiguration(String pathToConfig) {
    Configuration config = new Configuration();

    // check if the hdfs-site.xml is provided
    if (!Strings.isNullOrEmpty(pathToConfig)) {
      File file = new File(pathToConfig);
      if (file.exists() && file.isFile()) {
        log.info("Using XML config found at {}", pathToConfig);
        config.addResource(file.toURI().toURL());
      } else {
        log.warn("XML config does not exist - {}", pathToConfig);
      }
    } else {
      log.info("XML config not provided");
    }
    return config;
  }

  private static String getHdfsPrefix(String pathToConfig) {
    String hdfsPrefixToUse = null;
    if (!Strings.isNullOrEmpty(pathToConfig)) {
      Configuration hdfsSite = getHdfsConfiguration(pathToConfig);
      hdfsPrefixToUse = hdfsSite.get("fs.default.name");
      if (hdfsPrefixToUse == null) {
        hdfsPrefixToUse = hdfsSite.get("fs.defaultFS");
      }
    }
    return hdfsPrefixToUse;
  }
}
