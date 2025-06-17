package org.gbif.pipelines.core.interpreters.model;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;

public interface LocationRecord {

    String getCountryCode();
    IssueRecord getIssues();
    String getPublishingCountry();
    boolean getHasCoordinate();
    Double getDecimalLatitude();
    Double getDecimalLongitude();
    void addIssue(OccurrenceIssue occurrenceIssue);
    void addIssue(Collection<String> occurrenceIssue);
    void setContinent(Object o);
    void setCoordinatePrecision(Double result);
    void setCoordinateUncertaintyInMeters(Double result);
    void setDepth(Double value);
    void setDepthAccuracy(Double accuracy);
    void setDistanceFromCentroidInMeters(@NotNull Double aDouble);
    void setElevation(Double value);
    void setElevationAccuracy(Double accuracy);
    void setFootprintWKT(String result);
    void setGadm(GadmFeatures gadmFeatures);
    void setGbifRegion(String s);
    void setGeoreferencedBy(List<String> list);
    void setHasGeospatialIssue(boolean b);
    void setHigherGeography(List<String> list);
    void setLocality(String s);
    void setMaximumDepthInMeters(Double payload);
    void setMaximumDistanceAboveSurfaceInMeters(Double payload);
    void setMaximumElevationInMeters(Double payload);
    void setMinimumDepthInMeters(Double payload);
    void setMinimumDistanceAboveSurfaceInMeters(Double payload);
    void setMinimumElevationInMeters(Double payload);
    void setPublishedByGbifRegion(String s);
    void setPublishingCountry(@NotNull String s);
    void setRepatriated(boolean b);
    void setStateProvince(String s);

    void setCountry(String title);

    void setCountryCode(String iso2LetterCode);

    void setDecimalLatitude(Double lat);

    void setDecimalLongitude(Double lng);

    void setHasCoordinate(Boolean aTrue);

    Double getCoordinateUncertaintyInMeters();

    void setWaterBody(String s);
}
