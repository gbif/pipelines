package au.org.ala.outlier;

import static org.junit.Assert.*;

import au.org.ala.distribution.DistributionLayer;
import au.org.ala.distribution.DistributionServiceImpl;
import java.util.*;
import org.junit.Test;

public class DistributionOutlierTest {
  // String spatial_url = "http://devt.ala.org.au:8080/ws/";
  String spatial_url = "https://spatial-test.ala.org.au/ws/";
  String local_url = "http://devt.ala.org.au:8080/ws/";
  String lsidGreyNurseShark =
      "urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283";

  @Test
  public void getMultiLayers() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(local_url);
    try {
      List<DistributionLayer> layers = impl.findLayersByLsid(lsidGreyNurseShark);
      assertSame(1, layers.size());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void outliers() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(local_url);
    try {

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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void multiLayers() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(spatial_url);
    try {
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
              "urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283",
              points);
      assertSame(1, results.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void error_outliers() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(spatial_url);
    try {

      Map points = new HashMap();
      // decimalLatitude, decimalLongitude
      Map inPoint = new HashMap();
      inPoint.put("decimalLatitude", -32.136495);
      inPoint.put("decimalLongitude", 115.746027);
      points.put("e512c707-fe92-492c-b869-799c57388c45", inPoint);

      // urn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3A34b6d1d7-81be-46cf-b1e1-e80c68d26b34
      // Map<String, Double> results =
      // impl.outliers(URLEncoder.encode("urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283", StandardCharsets.UTF_8.toString()), points);
      Map<String, Double> results =
          impl.outliers(
              "urn:lsid:biodiversity.org.au:afd.taxon:34b6d1d7-81be-46cf-b1e1-e80c68d26b34",
              points);
      assertSame(1, results.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** return error by chance. Suspect layerStore and postgres issue */
  @Test
  public void error_outliers_2() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(spatial_url);
    try {

      Map points = new HashMap();
      // decimalLatitude, decimalLongitude
      points.put(
          "12296131-a31f-4216-85fa-396277d2f4ad",
          new HashMap() {
            {
              put("decimalLatitude", -19.332035);
              put("decimalLongitude", 146.755553);
            }
          });
      points.put(
          "373d5681-c70b-4688-a22b-622e64c3eac8",
          new HashMap() {
            {
              put("decimalLatitude", -33.078737);
              put("decimalLongitude", 151.608692);
            }
          });
      points.put(
          "4d56b58d-0d29-4829-9208-9d349c609099",
          new HashMap() {
            {
              put("decimalLatitude", -23.35);
              put("decimalLongitude", 150.55);
            }
          });
      points.put(
          "4c276c96-3c8d-4069-bf8f-f5d3759b4c6c",
          new HashMap() {
            {
              put("decimalLatitude", -30.383333);
              put("decimalLongitude", 153.05);
            }
          });
      points.put(
          "05f4b479-65a1-4bf6-898d-57ec41c0e6f3",
          new HashMap() {
            {
              put("decimalLatitude", -27.548127);
              put("decimalLongitude", 153.071967);
            }
          });
      points.put(
          "a3bbedcc-9d22-4430-a9db-510dd27e4678",
          new HashMap() {
            {
              put("decimalLatitude", -30.148182);
              put("decimalLongitude", 153.195528);
            }
          });
      points.put(
          "cfc8cc88-15be-4470-985a-3f6c3d329db4",
          new HashMap() {
            {
              put("decimalLatitude", -19.282777);
              put("decimalLongitude", 146.800833);
            }
          });
      points.put(
          "583a310b-cbb6-44d4-954d-d4e1eac7fa3b",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "14b24792-bc00-4b96-bad1-76feaf58da94",
          new HashMap() {
            {
              put("decimalLatitude", -17.381555);
              put("decimalLongitude", 145.384942);
            }
          });

      Map inPoint = new HashMap();
      inPoint.put("decimalLatitude", -23.35);
      inPoint.put("decimalLongitude", 150.55);
      points.put("8f729f6e-5b74-4a05-a01b-0a81c654aba9", inPoint);
      points.put(
          "3a2e6757-0dc8-4094-a30e-e7e2ccaeedf9",
          new HashMap() {
            {
              put("decimalLatitude", -27.948758);
              put("decimalLongitude", 153.195111);
            }
          });
      points.put(
          "ca985114-3104-4c3a-a706-ccf241a7ff59",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "82840e46-88fa-434a-986f-10091d96ea7c",
          new HashMap() {
            {
              put("decimalLatitude", -23.392031);
              put("decimalLongitude", 150.502029);
            }
          });
      points.put(
          "ca985114-3104-4c3a-a706-ccf241a7ff59",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "7d082056-4712-4d71-8b4c-b779e3910c87",
          new HashMap() {
            {
              put("decimalLatitude", -27.941442);
              put("decimalLongitude", 153.199367);
            }
          });

      // urn%3Alsid%3Abiodiversity.org.au%3Aafd.taxon%3A34b6d1d7-81be-46cf-b1e1-e80c68d26b34

      Map<String, Double> results =
          impl.outliers(
              "urn:lsid:biodiversity.org.au:afd.taxon:21686993-4d8f-4ae5-8f81-017c8528c6d5",
              points);

      // Test grey shark on local
      // Map<String, Double> results = impl.outliers("
      // urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283", points);

      assertSame(0, results.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** return error by chance. Suspect layerStore and postgres issue */
  @Test
  public void error_test_local() {
    DistributionServiceImpl impl = DistributionServiceImpl.init(local_url);
    try {

      Map points = new HashMap();
      // decimalLatitude, decimalLongitude
      points.put(
          "12296131-a31f-4216-85fa-396277d2f4ad",
          new HashMap() {
            {
              put("decimalLatitude", -17.54858);
              put("decimalLongitude", 131.471238);
            }
          });
      points.put(
          "373d5681-c70b-4688-a22b-622e64c3eac8",
          new HashMap() {
            {
              put("decimalLatitude", -33.078737);
              put("decimalLongitude", 151.608692);
            }
          });
      points.put(
          "4d56b58d-0d29-4829-9208-9d349c609099",
          new HashMap() {
            {
              put("decimalLatitude", -23.35);
              put("decimalLongitude", 150.55);
            }
          });
      points.put(
          "4c276c96-3c8d-4069-bf8f-f5d3759b4c6c",
          new HashMap() {
            {
              put("decimalLatitude", -30.383333);
              put("decimalLongitude", 153.05);
            }
          });
      points.put(
          "05f4b479-65a1-4bf6-898d-57ec41c0e6f3",
          new HashMap() {
            {
              put("decimalLatitude", -27.548127);
              put("decimalLongitude", 153.071967);
            }
          });
      points.put(
          "a3bbedcc-9d22-4430-a9db-510dd27e4678",
          new HashMap() {
            {
              put("decimalLatitude", -30.148182);
              put("decimalLongitude", 153.195528);
            }
          });
      points.put(
          "cfc8cc88-15be-4470-985a-3f6c3d329db4",
          new HashMap() {
            {
              put("decimalLatitude", -19.282777);
              put("decimalLongitude", 146.800833);
            }
          });
      points.put(
          "583a310b-cbb6-44d4-954d-d4e1eac7fa3b",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "14b24792-bc00-4b96-bad1-76feaf58da94",
          new HashMap() {
            {
              put("decimalLatitude", -17.381555);
              put("decimalLongitude", 145.384942);
            }
          });

      points.put(
          "ca985114-3104-4c3a-a706-ccf241a7ff59",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "82840e46-88fa-434a-986f-10091d96ea7c",
          new HashMap() {
            {
              put("decimalLatitude", -23.392031);
              put("decimalLongitude", 150.502029);
            }
          });
      points.put(
          "ca985114-3104-4c3a-a706-ccf241a7ff59",
          new HashMap() {
            {
              put("decimalLatitude", -28.840865);
              put("decimalLongitude", 153.449584);
            }
          });
      points.put(
          "7d082056-4712-4d71-8b4c-b779e3910c87",
          new HashMap() {
            {
              put("decimalLatitude", -27.941442);
              put("decimalLongitude", 153.199367);
            }
          });

      // Test grey shark on local
      Map<String, Double> results =
          impl.outliers(
              "urn:lsid:biodiversity.org.au:afd.taxon:0c3e2403-05c4-4a43-8019-30e6d657a283",
              points);

      assertSame(9, results.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // [SPATIAL-SERVICE] 13:26:51 a.o.a.l.DistributionController : Runtime error in calculating
  // outliers of lsid: urn:lsid:biodiversity.org.au:afd.taxon:21686993-4d8f-4ae5-8f81-017c8528c6d5
  // [8f729f6e-5b74-4a05-a01b-0a81c654aba9:[decimalLongitude:150.55, decimalLatitude:-23.35],
  // 3a2e6757-0dc8-4094-a30e-e7e2ccaeedf9:[decimalLongitude:153.195111, decimalLatitude:-27.948758],
  // ca985114-3104-4c3a-a706-ccf241a7ff59:[decimalLongitude:153.449584, decimalLatitude:-28.840865],
  // 82840e46-88fa-434a-986f-10091d96ea7c:[decimalLongitude:150.502029, decimalLatitude:-23.392031],
  // 7d082056-4712-4d71-8b4c-b779e3910c87:[decimalLongitude:153.199367, decimalLatitude:-27.941442],
  // 14b24792-bc00-4b96-bad1-76feaf58da94:[decimalLongitude:145.384942, decimalLatitude:-17.381555],
  // 583a310b-cbb6-44d4-954d-d4e1eac7fa3b:[decimalLongitude:153.449584, decimalLatitude:-28.840865],
  // cfc8cc88-15be-4470-985a-3f6c3d329db4:[decimalLongitude:146.800833, decimalLatitude:-19.282777],
  // a3bbedcc-9d22-4430-a9db-510dd27e4678:[decimalLongitude:153.195528, decimalLatitude:-30.148182],
  // 05f4b479-65a1-4bf6-898d-57ec41c0e6f3:[decimalLongitude:153.071967, decimalLatitude:-27.548127],
  // 4c276c96-3c8d-4069-bf8f-f5d3759b4c6c:[decimalLongitude:153.05, decimalLatitude:-30.383333],
  // 4d56b58d-0d29-4829-9208-9d349c609099:[decimalLongitude:150.55, decimalLatitude:-23.35],
  // 373d5681-c70b-4688-a22b-622e64c3eac8:[decimalLongitude:151.608692, decimalLatitude:-33.078737],
  // 12296131-a31f-4216-85fa-396277d2f4ad:[decimalLongitude:146.755553, decimalLatitude:-19.332035]]
  // [SPATIAL-SERVICE] 13:26:51 a.o.a.l.DistributionController : java.lang.NumberFormatException:
  // For input string: ""9""

}
