package org.gbif.pipelines.core.parsers.location.cache;

import java.awt.image.BufferedImage;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.geocode.GeocodeRequest;

/** A cache which uses a bitmap to cache coordinate lookups. */
@Slf4j
public class BinaryBitmapLookup {

  // World map image lookup
  private final BufferedImage img;
  private static final int BORDER = 0x000000;
  private static final int NOTHING = 0xFFFFFF;
  private final int imgWidth;
  private final int imgHeight;
  private final String kvStoreType;

  @SneakyThrows
  private BinaryBitmapLookup(BufferedImage img, String kvStoreType) {
    this.img = img;
    this.imgHeight = img != null ? img.getHeight() : -1;
    this.imgWidth = img != null ? img.getWidth() : -1;
    this.kvStoreType = kvStoreType;
  }

  public static BinaryBitmapLookup create(@NonNull BufferedImage img, String kvStoreType) {
    return new BinaryBitmapLookup(img, kvStoreType);
  }

  /**
   * Check the colour of a pixel from the map image to determine the country. <br>
   * Other than the special cases, the colours are looked up using the web service the first time
   * they are found.
   *
   * @return Locations or null if the bitmap can't answer.
   */
  public boolean intersects(GeocodeRequest latLng) {
    double lat = latLng.getLat();
    double lng = latLng.getLng();
    // Convert the latitude and longitude to x,y coordinates on the image.
    // The axes are swapped, and the image's origin is the top left.
    int x = (int) Math.round((lng + 180d) / 360d * (imgWidth - 1));
    int y = imgHeight - 1 - (int) Math.round((lat + 90d) / 180d * (imgHeight - 1));

    int colour = img.getRGB(x, y) & 0x00FFFFFF; // Ignore possible transparency.

    if (log.isDebugEnabled()) {
      String hex = String.format("#%06x", colour);
      log.debug(
          "[{}] LatLong {},{} has pixel {},{} with colour {}", kvStoreType, lat, lng, x, y, hex);
    }

    switch (colour) {
      case BORDER:
        return true;

      case NOTHING:
        return false;

      default:
        return true;
    }
  }
}
