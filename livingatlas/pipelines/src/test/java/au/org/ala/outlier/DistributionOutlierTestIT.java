package au.org.ala.outlier;

import static org.junit.Assert.*;

import au.org.ala.distribution.DistributionLayer;
import au.org.ala.distribution.DistributionServiceImpl;
import au.org.ala.util.IntegrationTestUtils;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Test;

public class DistributionOutlierTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  String lsidGreyNurseShark =
      "https://biodiversity.org.au/afd/taxa/0c3e2403-05c4-4a43-8019-30e6d657a283";

  @Test
  public void getMultiLayers() throws Exception {
    DistributionServiceImpl impl =
        DistributionServiceImpl.init(itUtils.getConfig().getSamplingService().getWsUrl());
    List<DistributionLayer> layers = impl.findLayersByLsid(lsidGreyNurseShark);
    assertSame(1, layers.size());
  }

  @Test
  public void outliers() throws Exception {
    DistributionServiceImpl impl =
        DistributionServiceImpl.init(itUtils.getConfig().getSamplingService().getWsUrl());
    Map points = new HashMap();
    // decimalLatitude, decimalLongitude
    Map inPoint = new HashMap();
    inPoint.put("decimalLatitude", -17.54858);
    inPoint.put("decimalLongitude", 131.471238);
    points.put("2eb7cda9-f248-4e9e-89b7-44db7312e58a", inPoint);

    Map outPoint = new HashMap();
    outPoint.put("decimalLatitude", 26.1);
    outPoint.put("decimalLongitude", 127.5);
    points.put("6756a12e-d07c-4fc6-8637-a0036f0b76c9", outPoint);

    Map<String, Double> results = impl.outliers(lsidGreyNurseShark, points);
    assertSame(1, results.size());
  }

  @Test
  public void multiLayers() throws Exception {
    DistributionServiceImpl impl =
        DistributionServiceImpl.init(itUtils.getConfig().getSamplingService().getWsUrl());

    // Grey nurse shark
    Map points = new HashMap();
    // decimalLatitude, decimalLongitude
    Map inPoint = new HashMap();
    inPoint.put("decimalLatitude", -39.25);
    inPoint.put("decimalLongitude", 147.25);
    points.put("c27c235f-904c-4536-968a-3edcc43fb878", inPoint);

    Map outPoint = new HashMap();
    outPoint.put("decimalLatitude", -32.565);
    outPoint.put("decimalLongitude", 156.2983);
    points.put("aaf824f1-351f-40a4-b89c-e0260c96b4ae", outPoint);

    Map<String, Double> results =
        impl.outliers(
            "https://biodiversity.org.au/afd/taxa/0c3e2403-05c4-4a43-8019-30e6d657a283", points);
    assertSame(1, results.size());
  }

  @Test
  public void url_as_id_outliers() throws Exception {
    DistributionServiceImpl impl =
        DistributionServiceImpl.init(itUtils.getConfig().getSamplingService().getWsUrl());

    Map points = new HashMap();
    // decimalLatitude, decimalLongitude
    Map inPoint = new HashMap();
    inPoint.put("decimalLatitude", -37.151663);
    inPoint.put("decimalLongitude", 142.854663);
    points.put("e512c707-fe92-492c-b869-799c57388c45", inPoint);

    // Map<String, Double> results =
    // impl.outliers(URLEncoder.encode("urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283", StandardCharsets.UTF_8.toString()), points);
    Map<String, Double> results =
        impl.outliers("https://id.biodiversity.org.au/node/apni/2908371", points);
    assertSame(0, results.size());
  }
}
