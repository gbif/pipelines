package org.gbif.pipelines.factory;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Files;
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

  // Warning: Cannot load multiple images in a same class
  // Singleton has been applied on BufferedImageFactory.
  // If BufferedImageFactory.getInstance() has loaded 'bitmap.png', then it won't load another png.
  // If you need to load multiple images in a same parent class, you need to use static method
  // loadImageFile
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
  public static BufferedImage loadImageFile(String filePath) {
    Path path = Paths.get(filePath);
    if (!path.isAbsolute()) {
      try (InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
        if (is == null) {
          throw new FileNotFoundException("Can't load image from resource - " + filePath);
        }
        return ImageIO.read(is);
      }
    } else {
      try (InputStream is = Files.newInputStream(path)) {
        return ImageIO.read(is);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        throw new FileNotFoundException(
            "Unable to load image from absolute filePath - " + filePath);
      }
    }
  }
}
