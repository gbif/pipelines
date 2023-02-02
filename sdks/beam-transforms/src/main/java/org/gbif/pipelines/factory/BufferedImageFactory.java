package org.gbif.pipelines.factory;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.InputStream;
import javax.imageio.ImageIO;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
public class BufferedImageFactory {

  private static volatile BufferedImageFactory instance;

  private final BufferedImage image;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private BufferedImageFactory(HdfsConfigs configs, String imageCachePath) {
    this.image = loadImageFile(configs, imageCachePath);
  }

  // Warning: Cannot load multiple images in a same class
  // Singleton has been applied on BufferedImageFactory.
  // If BufferedImageFactory.getInstance() has loaded 'bitmap.png', then it won't load another png.
  // If you need to load multiple images in a same parent class, you need to use static method
  // loadImageFile
  public static BufferedImage getInstance(HdfsConfigs configs, String imageCachePath) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new BufferedImageFactory(configs, imageCachePath);
        }
      }
    }
    return instance.image;
  }

  @SneakyThrows
  public static BufferedImage loadImageFile(HdfsConfigs configs, String filePath) {
    Path path = new Path(filePath);
    log.info("Loading image for the image cache from {}", path);
    if (!path.isAbsolute()) {
      try (InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
        if (is == null) {
          throw new FileNotFoundException("Can't load image from resource - " + filePath);
        }
        return ImageIO.read(is);
      }
    } else {
      FileSystem fs = FsUtils.getFileSystem(configs, filePath);
      try (InputStream is = fs.open(path)) {
        return ImageIO.read(is);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        throw new FileNotFoundException(
            "Unable to load image from absolute filePath - " + filePath);
      }
    }
  }
}
