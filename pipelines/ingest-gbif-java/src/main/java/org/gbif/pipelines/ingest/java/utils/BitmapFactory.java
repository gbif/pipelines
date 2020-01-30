package org.gbif.pipelines.ingest.java.utils;

import java.awt.image.BufferedImage;
import java.util.Properties;

import org.gbif.pipelines.ingest.utils.FsUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.BITMAP_FILE_NAME;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.BITMAP_PROPERTY_NAME;

@Slf4j
public class BitmapFactory {

  private static volatile BitmapFactory instance;

  private final BufferedImage image;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private BitmapFactory(String hdfsSiteConfig, Properties properties) {
    this.image = FsUtils.loadImageFile(hdfsSiteConfig, properties.getProperty(BITMAP_PROPERTY_NAME, BITMAP_FILE_NAME));
  }

  public static BitmapFactory getInstance(String hdfsSiteConfig, Properties properties) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new BitmapFactory(hdfsSiteConfig, properties);
        }
      }
    }
    return instance;
  }

  public BufferedImage get() {
    return image;
  }

}
