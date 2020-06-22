package org.gbif.pipelines.parsers.parsers.location.cache;

import java.awt.image.BufferedImage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.kvs.geocode.LatLng;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** A cache which uses a bitmap to cache coordinate lookups. */
@Slf4j
public class GeocodeBitmapCache {

  private final Function<LatLng, GeocodeResponse> loadFn;

  // World map image lookup
  private final BufferedImage img;
  private static final int BORDER = 0x000000;
  private static final int EEZ = 0x888888;
  private static final int INTERNATIONAL_WATER = 0xFFFFFF;
  private final int imgWidth;
  private final int imgHeight;
  private final Map<Integer, GeocodeResponse> colourKey = new HashMap<>();

  @SneakyThrows
  private GeocodeBitmapCache(BufferedImage img, Function<LatLng, GeocodeResponse> loadFn) {
    this.loadFn = loadFn;
    this.img = img;
    this.imgHeight = img != null ? img.getHeight() : -1;
    this.imgWidth = img != null ? img.getWidth() : -1;
  }

  public static GeocodeBitmapCache create(@NonNull BufferedImage img, @NonNull Function<LatLng, GeocodeResponse> loadFn) {
    return new GeocodeBitmapCache(img, loadFn);
  }

  /**
   * Check the colour of a pixel from the map image to determine the country.
   * <br/>
   * Other than the special cases, the colours are looked up using the web service the first
   * time they are found.
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
      case EEZ:
        return null;

      case INTERNATIONAL_WATER:
        return new GeocodeResponse(Collections.emptyList());

      default:
        return getDefaultGeocodeResponse(lat, lng, x, y, colour, hex);
    }
  }

  private GeocodeResponse getDefaultGeocodeResponse(double lat, double lng, int x, int y, int colour, String hex) {

    GeocodeResponse locations;
    if (colourKey.containsKey(colour)) {
      locations = colourKey.get(colour);
      log.debug("Known colour {} (LL {},{}; pixel {},{}) is {}", hex, lat, lng, x, y, joinLocations(locations));
      return locations;
    }

    locations = loadFn.apply(LatLng.builder().withLatitude(lat).withLongitude(lng).build());
    // Don't store this if there aren't any locations.
    if (locations.getLocations().isEmpty()) {
      log.error("For colour {} (LL {},{}; pixel {},{}) the webservice gave zero locations.", hex, lat, lng, x, y);
    } else {
      // Don't store if the ISO code is -99; this code is used for some exceptional bits of territory (e.g. Baikonur Cosmodrome, the Korean DMZ).
      if ("-99".equals(locations.getLocations().iterator().next().getIsoCountryCode2Digit())) {
        log.info("New colour {} (LL {},{}; pixel {},{}); exceptional territory of {} will not be cached", hex, lat, lng,
            x, y, joinLocations(locations));
      } else {
        if (joinLocations(locations).length() > 2) {
          log.error("More than two countries for a colour! {} (LL {},{}; pixel {},{}); countries {}", hex, lat,
              lng, x, y, joinLocations(locations));
        } else {
          log.info("New colour {} (LL {},{}; pixel {},{}); remembering as {}", hex, lat, lng, x, y,
              joinLocations(locations));
          colourKey.put(colour, locations);
        }
      }
    }

    return locations;
  }

  private String joinLocations(GeocodeResponse loc) {
    return loc.getLocations()
        .stream()
        .map(Location::getIsoCountryCode2Digit)
        .distinct()
        .collect(Collectors.joining(", "));
  }

}
