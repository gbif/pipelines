package org.gbif.pipelines.kv;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.gbif.pipelines.parsers.config.model.KvConfig;

import javax.imageio.ImageIO;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BitmapFactory {

  private static volatile BitmapFactory instance;

  private final BufferedImage image;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private BitmapFactory(KvConfig config) {
    this.image = loadImageFile(config.getImagePath());
  }

  public static BufferedImage getInstance(KvConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new BitmapFactory(config);
        }
      }
    }
    return instance.image;
  }

  @SneakyThrows
  public BufferedImage loadImageFile(String filePath) {
    Path path = Paths.get(filePath);
    if (!path.isAbsolute()) {
      try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
        if (is == null) {
          throw new FileNotFoundException("Can't load image from resource - " + filePath);
        }
        return ImageIO.read(is);
      }
    }
    throw new FileNotFoundException("The image file doesn't exist - " + filePath);
  }
}
