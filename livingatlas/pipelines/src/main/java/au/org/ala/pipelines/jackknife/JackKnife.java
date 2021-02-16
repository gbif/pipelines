package au.org.ala.pipelines.jackknife;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

/** Code for running jackKnife against a set of float values. */
@Slf4j
public class JackKnife {

  /**
   * Takes a list of sampled values and returns the statistics for these results
   *
   * @param values array of values used for jackknife. Nulls permitted.
   * @param minSampleThreshold minimum number of values
   * @return null or JackKnifeStats
   */
  public static double[] jackknife(Double[] values, Integer minSampleThreshold) {
    // inclusive outlier range
    double maxValue, minValue;

    int nulls = 0;
    for (Double value : values) {
      if (value == null) {
        nulls++;
      }
    }

    double[] valuesWithoutNulls = new double[values.length - nulls];
    int pos = 0;
    for (Double value : values) {
      if (value != null) {
        valuesWithoutNulls[pos++] = value;
      }
    }

    // number of non-null values
    int n = valuesWithoutNulls.length;

    if (valuesWithoutNulls.length < minSampleThreshold) {
      return null;
    }

    Arrays.sort(valuesWithoutNulls);

    double min = valuesWithoutNulls[0];
    double max = valuesWithoutNulls[n - 1];

    double smean = 0;
    double sstd = 0;
    double srange = max - min;
    double threshold = ((0.95 * Math.sqrt(n) + 0.2) * (srange / 50.0));

    if (threshold <= 0) {
      return null;
    }

    for (double v : valuesWithoutNulls) {
      smean += v;
    }
    smean = smean / n;

    for (double v : valuesWithoutNulls) {
      sstd += Math.pow(v - smean, 2);
    }
    sstd = Math.sqrt(sstd / n);

    int minIdx = -1;
    int maxIdx = -1;

    for (int i = 0; i < n; i++) {
      double v = valuesWithoutNulls[i];
      double y;

      // values are sorted so a range check is not required for values[i+1] and values[i-1]
      if (v < smean) {
        y = (valuesWithoutNulls[i + 1] - v) * (smean - v);
        double c = y / sstd;

        if (c > threshold) {
          minIdx = i; // continue searching for a larger minIdx
        }
      } else if (v > smean) {
        y = (v - valuesWithoutNulls[i - 1]) * (v - smean);
        double c = y / sstd;

        if (c > threshold) {
          maxIdx = i;

          break; // smallest maxIdx found, stop searching
        }
      }
    }

    int outlierCount = 0;

    // set minimum outlier value
    minValue = valuesWithoutNulls[minIdx + 1];
    outlierCount += minIdx + 1;

    // set maximum outlier value
    if (maxIdx < 0) maxValue = valuesWithoutNulls[n - 1]; // larger than largest value
    else {
      maxValue = valuesWithoutNulls[maxIdx - 1];
      outlierCount += n - maxIdx;
    }

    // failsafe trigger
    if (outlierCount > n / 2) {
      return null;
    }

    return new double[] {minValue, maxValue};
  }
}
