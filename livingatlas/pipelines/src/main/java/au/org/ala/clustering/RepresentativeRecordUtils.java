package au.org.ala.clustering;

import java.util.*;
import org.gbif.clustering.parsers.OccurrenceFeatures;

/** Utility methods for identifying the representative record in cluster. */
public class RepresentativeRecordUtils {

  /**
   * Finds the representative record from the set provided. The representative record is chosen by
   * prioritising records by:
   *
   * <p>1) Coordinates 2) Date 3) First added to system
   *
   * @return OccurrenceFeatures considered the representative record.
   */
  public static HashKeyOccurrence findRepresentativeRecord(Set<HashKeyOccurrence> cluster) {

    // get highest resolution coordinates
    Set<HashKeyOccurrence> highestRanking = rankCoordPrecision(cluster);
    if (highestRanking.size() == 1) {
      return highestRanking.stream().findFirst().get();
    }

    // get highest resolution dates
    highestRanking = rankDatePrecision(highestRanking);
    if (highestRanking.size() == 1) {
      return highestRanking.stream().findFirst().get();
    }

    // otherwise, just pick one using a consistent method
    return pickRepresentative(highestRanking);
  }

  public static HashKeyOccurrence pickRepresentative(Set<HashKeyOccurrence> cluster) {
    return cluster.stream().min(Comparator.comparing(HashKeyOccurrence::getId)).get();
  }

  /**
   * Returns from the list supplied the list of the highest rank occurrences in terms of date
   * precision. Occurrences with a year, month and day will be ranked the highest. Occurrences with
   * just a year value or nothing will be ranked lowest.
   */
  public static Set<HashKeyOccurrence> rankDatePrecision(Set<HashKeyOccurrence> cluster) {

    Iterator<HashKeyOccurrence> iter = cluster.iterator();
    HashKeyOccurrence occurrenceFeatures = iter.next();
    int highestPrecision = determineDatePrecision(occurrenceFeatures);
    Set<HashKeyOccurrence> highestRanking = new HashSet<>();
    highestRanking.add(occurrenceFeatures);

    while (iter.hasNext()) {

      HashKeyOccurrence occurrenceFeatures1 = iter.next();
      int precision = determineDatePrecision(occurrenceFeatures1);
      if (precision == highestPrecision) {
        highestRanking.add(occurrenceFeatures1);
      } else if (precision > highestPrecision) {
        highestRanking.clear();
        highestRanking.add(occurrenceFeatures1);
        highestPrecision = precision;
      }
    }

    // return the highest
    return highestRanking;
  }

  /**
   * Returns a subset of the cluster containing the occurrence features with the highest record
   * precision.
   */
  public static Set<HashKeyOccurrence> rankCoordPrecision(Set<HashKeyOccurrence> cluster) {

    Iterator<HashKeyOccurrence> iter = cluster.iterator();
    HashKeyOccurrence occurrenceFeatures = iter.next();
    int highestPrecision = determineCoordPrecision(occurrenceFeatures);
    LinkedHashSet<HashKeyOccurrence> highestRanking = new LinkedHashSet<>();
    highestRanking.add(occurrenceFeatures);

    while (iter.hasNext()) {

      HashKeyOccurrence occurrenceFeatures1 = iter.next();
      int precision = determineCoordPrecision(occurrenceFeatures1);
      if (precision == highestPrecision) {
        highestRanking.add(occurrenceFeatures1);
      } else if (precision > highestPrecision) {
        highestRanking = new LinkedHashSet<>();
        highestRanking.add(occurrenceFeatures1);
        highestPrecision = precision;
      }
    }

    // return the highest
    return highestRanking;
  }

  /**
   * Reports the maximum number of decimal places that the lat/long are reported to Very coarse
   * means to determine precision (logic copied from biocache-store) TODO look for datum,
   * coordinateUncertaintyInMeters
   */
  public static Integer determineCoordPrecision(OccurrenceFeatures occurrenceFeatures) {

    if (occurrenceFeatures.getDecimalLatitude() == null
        || occurrenceFeatures.getDecimalLongitude() == null) {
      return 0;
    }
    Integer latp = extractNoDecPlaces(occurrenceFeatures.getDecimalLatitude());
    Integer lonp = extractNoDecPlaces(occurrenceFeatures.getDecimalLongitude());
    if (latp > lonp) {
      return latp;
    } else {
      return lonp;
    }
  }

  public static Integer extractNoDecPlaces(Double doubleValue) {
    String decimalPlaces = doubleValue.toString().split("\\.")[1];
    if (decimalPlaces.length() > 1) {
      return decimalPlaces.length();
    }
    if (decimalPlaces.length() == 1 && "0".equals(decimalPlaces)) {
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Reports precision of the date information as a number. A record with year, month and day = 3,
   * with a point removed for each component that is missing.
   */
  public static int determineDatePrecision(OccurrenceFeatures occurrenceFeatures) {

    int datePrecision = 0;
    if (occurrenceFeatures.getYear() != null) {
      datePrecision += 1;
    }

    if (occurrenceFeatures.getMonth() != null) {
      datePrecision += 1;
    }

    if (occurrenceFeatures.getDay() != null) {
      datePrecision += 1;
    }

    return datePrecision;
  }

  /**
   * Takes the list of paired occurrences and creates clusters from the pairs. i.e. If we have a
   * pairs (A,B) and (B, C), this will form a cluster (A, B, C).
   */
  public static List<Set<HashKeyOccurrence>> createClusters(List<ClusterPair> pairs) {

    List<Set<HashKeyOccurrence>> clusters = new ArrayList<>();

    for (ClusterPair pair : pairs) {
      boolean added = false;

      // iterate through each cluster
      for (Set<HashKeyOccurrence> cluster : clusters) {
        boolean present = cluster.contains(pair.getO1()) || cluster.contains(pair.getO2());
        if (present) {
          added = true;
          cluster.add(pair.getO1());
          cluster.add(pair.getO2());
        }
      }

      if (!added) {
        // create a cluster
        Set<HashKeyOccurrence> newCluster = new HashSet<>();
        newCluster.add(pair.getO1());
        newCluster.add(pair.getO2());
        clusters.add(newCluster);
      }
    }
    return clusters;
  }
}
