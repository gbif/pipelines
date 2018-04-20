package org.gbif.pipelines.core.parsers.location;

import java.util.Objects;

/**
 * Package private class used to encapsulate a LatLng parsing request and use it as key in map/caches.
 */
class RawLatLng {

    private final String lat;

    private final String lng;

    public String getLat() {
        return lat;
    }

    public String getLng() {
        return lng;
    }

    private RawLatLng(String lat, String lng) {
        this.lat = lat;
        this.lng = lng;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RawLatLng rawLatLng = (RawLatLng) o;
        return Objects.equals(lat, rawLatLng.lat) &&
                Objects.equals(lng, rawLatLng.lng);
    }

    @Override
    public int hashCode() {

        return Objects.hash(lat, lng);
    }

    public static RawLatLng of(String lat, String lng) {
        return new RawLatLng(lat,lng);
    }
}
