package au.org.ala.utils;

import static org.apache.batik.transcoder.image.ImageTranscoder.KEY_BACKGROUND_COLOR;

import java.awt.Color;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import lombok.NoArgsConstructor;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;

/**
 * It is a simple workaround to generate coloured SVG for a SHP file.
 *
 * <p>SHP files need to loaded to Postgres first.
 *
 * <p>A dockerised postgres needs to setup first. database: eez; user: eez); password: eez);
 */
@NoArgsConstructor
public class BitMapGenerator {

  // It does not seem worth using a templating engine for a single "template".  Welcome to 1995!
  private static final String SVG_HEADER =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<!DOCTYPE svg PUBLIC '-//W3C//DTD SVG 1.0//EN' 'http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd'>\n"
          +
          // geometricPrecision has anti-aliasing, but crispEdges misses out tiny shapes.
          "<svg shape-rendering=\"geometricPrecision\" "
          +
          // This gives us a 7200×3600 image, with the "units" (pixels) on a scale -180°–180°,
          // -90°–90°.
          // Therefore, 1px = 1°.
          "height=\"3600\" width=\"7200\" viewBox=\"-180 -90 360 180\" xmlns=\"http://www.w3.org/2000/svg\">\n"
          + "\n"
          + "  <style type=\"text/css\">\n"
          + "    path {\n"
          + "      stroke: #000000;\n"
          +
          // Very thin outlines are used (this is 0.01°), and increased as required later.
          "      stroke-width: 0.01px;\n"
          + "      stroke-linecap: round;\n"
          + "      stroke-linejoin: round;\n"
          + "      fill: none;\n"
          + "    }\n"
          + "  </style>\n";

  private static final String HOLLOW_PATH_FORMAT = "  <path id='%s' d='%s'/>\n";

  private static final String FILLED_PATH_FORMAT = "  <path id='%s' style='fill: %s' d='%s'/>\n";

  private static final String SVG_FOOTER = "</svg>\n";

  public String generateSVG(String layer, String idName) throws Exception {
    String url = "jdbc:postgresql://127.0.0.1/eez";
    Properties props = new Properties();
    props.setProperty("user", "eez");
    props.setProperty("password", "eez");
    props.setProperty("ssl", "false");
    Connection conn = DriverManager.getConnection(url, props);
    System.out.println("Successfully Connected.");

    String svg = "";
    svg += SVG_HEADER;

    Map<String, String> colourKey = new HashMap<>();

    Statement stmt = conn.createStatement();

    String idSQL = String.format("SELECT DISTINCT %s as id FROM %s;", idName, layer);
    ResultSet idRs = stmt.executeQuery(idSQL);

    while (idRs.next()) {
      String id = idRs.getString("id");
      Random random = new Random();
      // create a big random number - maximum is ffffff (hex) = 16777215 (dez)
      int nextInt = random.nextInt(0xffffff + 1);
      // format it as hexadecimal string (with hashtag and leading zeros)
      String colorCode = String.format("#%06x", nextInt);
      colourKey.put(id, colorCode);
    }
    idRs.close();

    String svgSQL =
        String.format(
            "SELECT %s as id, ST_AsSVG(geom, 0, 4) as svg FROM %s order by id;",
            idName, layer);

    ResultSet rs = stmt.executeQuery(svgSQL);

    while (rs.next()) {
      String id = rs.getString("id");
      String svgString = rs.getString("svg");
      String svgOutout = String.format(FILLED_PATH_FORMAT, id, colourKey.get(id), svgString);
      svg += svgOutout;
    }

    svg += SVG_FOOTER;

    rs.close();
    stmt.close();
    conn.close();

    return svg;
  }

  public void writeBitmap(String content, String outputFolder, String fileName) throws Exception {
    String svgFile = outputFolder + fileName + ".svg";
    BufferedWriter writer = new BufferedWriter(new FileWriter(svgFile));
    writer.write(content);
    System.out.println("Successfully generated.");
    writer.close();

    System.out.println(svgFile + " is generated.");
    System.out.println("Converting SVG to PNG");

    String pngFile = outputFolder + fileName + ".png";
    Files.deleteIfExists(Paths.get(pngFile));
    Path filledPngFile = Files.createFile(Paths.get(pngFile));
    try (OutputStream pngOut = new FileOutputStream(filledPngFile.toFile())) {
      TranscoderInput svgImage = new TranscoderInput(svgFile);
      TranscoderOutput pngImage = new TranscoderOutput(pngOut);
      PNGTranscoder pngTranscoder = new PNGTranscoder();
      pngTranscoder.addTranscodingHint(KEY_BACKGROUND_COLOR, Color.white);
      pngTranscoder.transcode(svgImage, pngImage);
    }
    System.out.println(pngFile + " is generated.");
  }

  /**
   * BitMapGenerator cw_state_poly feature /Users/Shared/Relocated Items/Security/data/sds-shp/
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Error: args are incorrect!");
      System.out.println(
          "Example: BitMapGenerator cw_state_poly feature /data/sds-shp/");
    }
    try {
      String layer = args[0];
      String idName = args[1];
      String outputFolder = args[2];

      BitMapGenerator bmg = new BitMapGenerator();
      String svg = bmg.generateSVG(layer, idName);
      // Use layer name as default file name.
      bmg.writeBitmap(svg, outputFolder, layer);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * The circumference of a parallel (line of longitude at a particular latitude) in kilometres.
   *
   * <p>Earth approximated as a sphere, which is sufficient for these bitmaps.
   */
  private static double lengthParallelKm(double latitude) {
    return 2d * Math.PI * 6378.137 /* Earth radius */ * Math.cos(Math.toRadians(latitude));
  }

  /** Length of N kilometres in pixels, on a 7200×3600 pixel map. */
  private static double kmToPx(double latitude, double n_km) {
    return n_km / (lengthParallelKm(latitude) / 7200d);
  }

  private static double kmToPx(double n_km) {
    return n_km / (lengthParallelKm(0) / 7200d);
  }
}
