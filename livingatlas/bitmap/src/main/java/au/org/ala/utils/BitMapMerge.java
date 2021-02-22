package au.org.ala.utils;

import static java.awt.image.BufferedImage.TYPE_INT_RGB;

import com.google.common.base.Stopwatch;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import lombok.extern.slf4j.Slf4j;

/**
 * Merge multiple bitmaps If there are overlapped zones, this part of colour will be replaced with
 * another unique colour
 */
@Slf4j
public class BitMapMerge {

  private static final int HEIGHT = 3600;
  private static final int WIDTH = 7200;

  private final Map<String, Integer> colourKey = new HashMap<>();
  private final Set<Integer> usedColours = new HashSet<>();

  private int lastColour = 0;
  private int inc = 0;

  /** Combines the bitmaps for every layer into a single bitmap, for use as a client cache. */
  public void combineAllBitmaps(String target, String... bitmaps) throws IOException {
    // Path pngFile = Paths.get(target);

    log.info("Generating combined layer bitmap.");
    Stopwatch sw = Stopwatch.createStarted();

    BufferedImage combined = new BufferedImage(WIDTH, HEIGHT, TYPE_INT_RGB);

    BufferedImage[] images = new BufferedImage[bitmaps.length];
    for (int i = 0; i < bitmaps.length; i++) {
      log.info("Loading {}", bitmaps[i]);
      images[i] = ImageIO.read(new FileInputStream(bitmaps[i]));
      assert (HEIGHT == combined.getHeight());
      assert (WIDTH == combined.getWidth());
    }

    for (int x = 0; x < WIDTH; x++) {
      for (int y = 0; y < HEIGHT; y++) {
        StringBuilder key = new StringBuilder();
        for (BufferedImage image : images) {
          int colour = image.getRGB(x, y) & 0x00FFFFFF;
          if (colour == 0x000000) {
            key.append("BLACK");
            break;
          }
          if (colour == 0xFFFFFF) {
            key.append("W");
          } else {
            key.append(colour);
          }
        }
        combined.setRGB(x, y, getColour(key.toString()));
      }
    }
    log.info("Writing to {}", target);
    File pngFile = new File(target);
    boolean isCreated = pngFile.createNewFile();
    if (isCreated) {
      try (OutputStream pngOut = new FileOutputStream(pngFile, false)) {
        ImageIO.write(combined, "PNG", pngOut);
        log.info(
            "Combined bitmap with {} colours completed in {} s",
            colourKey.size(),
            sw.elapsed(TimeUnit.SECONDS));
      }
    } else {
      log.error("PNG file {} wasn't created!", target);
    }
  }

  private synchronized int getColour(String key) {
    if (inc == 0) {
      usedColours.add(0x000000);
      usedColours.add(0xFFFFFF);
      // Parameter will need changing if the number of polygons increases significantly.
      // (The idea is to go through the FFFFFF colours ~three times, so nearby polygons aren't such
      // close colours.)
      for (inc = 2400; inc < 20000; inc++) {
        if (0xFFFFFF % inc == 3) break;
      }
    }

    if (key.matches("^W+$")) {
      return 0xFFFFFF;
    }

    if (key.equals("BLACK")) {
      return 0x000000;
    }

    if (!colourKey.containsKey(key)) {
      lastColour = (lastColour + inc) % 0xFFFFFF;
      assert !usedColours.contains(lastColour);
      colourKey.put(key, lastColour);
      usedColours.add(lastColour);
    }
    return colourKey.get(key);
  }

  /**
   * "/data/sds-shp/combined.png", "/data/sds-shp/ffez.png", "/data/sds-shp/quarantine_zone.png"
   * Merge the rest pngs to the first.
   */
  public static void main(String[] args) throws IOException {
    BitMapMerge bm = new BitMapMerge();
    bm.combineAllBitmaps(
        "/data/sds-shp/combined.png",
        "/data/sds-shp/ffez.png",
        "/data/sds-shp/quarantine_zone.png");
  }
}
