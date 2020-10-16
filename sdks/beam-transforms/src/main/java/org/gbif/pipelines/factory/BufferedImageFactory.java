package org.gbif.pipelines.factory;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.imageio.ImageIO;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferedImageFactory {

  private static volatile BufferedImageFactory instance;

  private final BufferedImage image;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private BufferedImageFactory(String imageCachePath) {
    this.image = loadImageFile(imageCachePath);
  }

  public static BufferedImage getInstance(String imageCachePath) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new BufferedImageFactory(imageCachePath);
        }
      }
    }
    return instance.image;
  }

  @SneakyThrows
  public BufferedImage loadImageFile(String filePath) {
    Path path = Paths.get(filePath);
    if (!path.isAbsolute()) {
      try (InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
        if (is == null) {
          throw new FileNotFoundException("Can't load image from resource - " + filePath);
        }
        return ImageIO.read(is);
      }
    }
    throw new FileNotFoundException("The image file doesn't exist - " + filePath);
  }
}
