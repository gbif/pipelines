package org.gbif.pipelines.core.parsers.location.cache;

import java.awt.image.BufferedImage;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

/** A cache which uses a bitmap to cache coordinate lookups. */
@Slf4j
public class GeocodeBitmapCache {

  private final Function<LatLng, GeocodeResponse> loadFn;

  // World map image lookup
  private final BufferedImage img;
  private static final int BORDER = 0x000000;
  private static final int NOTHING = 0xFFFFFF;
  private final int imgWidth;
  private final int imgHeight;
  private final Map<Integer, GeocodeResponse> colourKey = new ConcurrentHashMap<>();

  @SneakyThrows
  private GeocodeBitmapCache(BufferedImage img, Function<LatLng, GeocodeResponse> loadFn) {
    this.loadFn = loadFn;
    this.img = img;
    this.imgHeight = img != null ? img.getHeight() : -1;
    this.imgWidth = img != null ? img.getWidth() : -1;
  }

  public static GeocodeBitmapCache create(
      @NonNull BufferedImage img, @NonNull Function<LatLng, GeocodeResponse> loadFn) {
    return new GeocodeBitmapCache(img, loadFn);
  }

  /**
   * Check the colour of a pixel from the map image to determine the country. <br>
   * Other than the special cases, the colours are looked up using the web service the first time
   * they are found.
   *
   * @return Locations or null if the bitmap can't answer.
   */
  public GeocodeResponse getFromBitmap(LatLng latLng) {
    double lat = latLng.getLatitude();
    double lng = latLng.getLongitude();
    // Convert the latitude and longitude to x,y coordinates on the image.
    // The axes are swapped, and the image's origin is the top left.
    int x = (int) Math.round((lng + 180d) / 360d * (imgWidth - 1));
    int y = imgHeight - 1 - (int) Math.round((lat + 90d) / 180d * (imgHeight - 1));

    int colour = img.getRGB(x, y) & 0x00FFFFFF; // Ignore possible transparency.

    String hex = String.format("#%06x", colour);
    log.debug("LatLong {},{} has pixel {},{} with colour {}", lat, lng, x, y, hex);

    switch (colour) {
      case BORDER:
        return null;

      case NOTHING:
        return new GeocodeResponse(Collections.emptyList());

      default:
        return getDefaultGeocodeResponse(lat, lng, x, y, colour, hex);
    }
  }

  private GeocodeResponse getDefaultGeocodeResponse(
      double lat, double lng, int x, int y, int colour, String hex) {

    GeocodeResponse locations;
    if (colourKey.containsKey(colour)) {
      locations = colourKey.get(colour);
      log.debug("Known colour {} (LL {},{}; pixel {},{})", hex, lat, lng, x, y);
      return locations;
    }

    locations = loadFn.apply(LatLng.builder().withLatitude(lat).withLongitude(lng).build());
    // Don't store this if there aren't any locations.
    if (locations.getLocations().isEmpty()) {
      log.error(
          "For colour {} (LL {},{}; pixel {},{}) the webservice gave zero locations.",
          hex,
          lat,
          lng,
          x,
          y);
    } else {
      log.info(
          "New colour {} (LL {},{}; pixel {},{}); remembering as {}",
          hex,
          lat,
          lng,
          x,
          y,
          joinLocations(locations));
      colourKey.put(colour, locations);
    }

    return locations;
  }

  private String joinLocations(GeocodeResponse loc) {
    return loc.getLocations().stream()
        .map(Location::getId)
        .distinct()
        .collect(Collectors.joining(", "));
  }
}
