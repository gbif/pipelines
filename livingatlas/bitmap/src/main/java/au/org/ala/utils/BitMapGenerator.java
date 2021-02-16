package au.org.ala.utils;

import com.google.common.base.Strings;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import javax.imageio.ImageIO;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.ImageTranscoder;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.http.annotation.Obsolete;

/**
 * It is a simple workaround to generate coloured SVG for a SHP file. NOTE: There are no overlapped
 * zones in this SHP file
 *
 * <p>SHP files need to loaded to Postgres first.
 *
 * <p>A dockerised postgres needs to setup first. database: eez; user: eez); password: eez);
 */
@Slf4j
@AllArgsConstructor
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

  private final String url;
  private final String db;
  private final String password;
  private final String user;

  public String[] generateSVG(String layer, String idName) throws SQLException, URISyntaxException {
    URI uri = new URI(url + "/" + db);

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("ssl", "false");
    StringBuilder filledSvg = new StringBuilder();
    StringBuilder hollowSvg = new StringBuilder();
    try (Connection conn = DriverManager.getConnection(uri.toString(), props);
        Statement stmt = conn.createStatement()) {
      log.info("Successfully Connected.");

      filledSvg.append(SVG_HEADER);
      hollowSvg.append(SVG_HEADER);

      Map<String, String> colourKey = new HashMap<>();

      String idSQL = String.format("SELECT DISTINCT %s as id FROM %s;", idName, layer);
      try (ResultSet idRs = stmt.executeQuery(idSQL)) {
        while (idRs.next()) {
          String id = idRs.getString("id");
          Random random = new Random();
          // create a big random number - maximum is ffffff (hex) = 16777215 (dez)
          int nextInt = random.nextInt(0xffffff + 1);
          // format it as hexadecimal string (with hashtag and leading zeros)
          String colorCode = String.format("#%06x", nextInt);
          colourKey.put(id, colorCode);
        }
      }

      String svgSQL =
          String.format(
              "SELECT %s as id, ST_AsSVG(geom, 0, 4) as svg FROM %s order by id;", idName, layer);
      try (ResultSet rs = stmt.executeQuery(svgSQL)) {

        while (rs.next()) {
          String id = rs.getString("id").replace("\'", " ").replace("\"", " ");
          String svgString = rs.getString("svg");
          String svgFilledOutout =
              String.format(FILLED_PATH_FORMAT, id, colourKey.get(id), svgString);
          String svgHollowOutout = String.format(HOLLOW_PATH_FORMAT, id, svgString);
          filledSvg.append(svgFilledOutout);
          hollowSvg.append(svgHollowOutout);
        }

        filledSvg.append(SVG_FOOTER);
        hollowSvg.append(SVG_FOOTER);
      }
    }

    return new String[] {filledSvg.toString(), hollowSvg.toString()};
  }

  public void mergeTwoBitmaps(String[] twoSvgs, String outputFolder, String layerName)
      throws IOException, TranscoderException {

    Path filledPngFile = Files.createTempFile(layerName, "-filled.png");
    Path hollowPngFile = Files.createTempFile(layerName, "-hollow.png");
    log.info("Generating bitmap for {}", layerName);

    Path filledSvgFile = Files.createTempFile(layerName, "-filled.svg");
    Path hollowSvgFile = Files.createTempFile(layerName, "-hollow.svg");
    log.info("→ Generating SVG for {} as {} and {}", layerName, filledSvgFile, hollowSvgFile);
    try (Writer filledSvg =
            new OutputStreamWriter(
                new FileOutputStream(filledSvgFile.toFile()), StandardCharsets.UTF_8);
        Writer hollowSvg =
            new OutputStreamWriter(
                new FileOutputStream(hollowSvgFile.toFile()), StandardCharsets.UTF_8)) {
      filledSvg.write(twoSvgs[0]);
      hollowSvg.write(twoSvgs[1]);
    }

    log.info("→ Converting SVG to PNG for {} as {}", layerName, hollowPngFile);
    try (OutputStream pngOut = new FileOutputStream(hollowPngFile.toFile())) {
      TranscoderInput svgImage = new TranscoderInput(hollowSvgFile.toString());
      TranscoderOutput pngImage = new TranscoderOutput(pngOut);
      PNGTranscoder pngTranscoder = new PNGTranscoder();
      pngTranscoder.addTranscodingHint(ImageTranscoder.KEY_BACKGROUND_COLOR, Color.white);
      pngTranscoder.transcode(svgImage, pngImage);
    }

    log.info("→ Converting SVG to PNG for {} as {}", layerName, filledPngFile);
    try (OutputStream pngOut = new FileOutputStream(filledPngFile.toFile())) {
      TranscoderInput svgImage = new TranscoderInput(filledSvgFile.toString());
      TranscoderOutput pngImage = new TranscoderOutput(pngOut);
      PNGTranscoder pngTranscoder = new PNGTranscoder();
      pngTranscoder.addTranscodingHint(ImageTranscoder.KEY_BACKGROUND_COLOR, Color.white);
      pngTranscoder.transcode(svgImage, pngImage);
    }

    Path pngFile = Paths.get(outputFolder).resolve(layerName + ".png");
    if (pngFile.toFile().exists()) {
      log.error(
          "Won't overwrite {}, remove it first if you want to regenerate it (slow).", pngFile);
      return;
    }
    log.info("→ Combining both PNGs for {} as {}", layerName, pngFile);
    BufferedImage filled = ImageIO.read(filledPngFile.toFile());
    BufferedImage hollow = ImageIO.read(hollowPngFile.toFile());

    int height = filled.getHeight();
    int width = filled.getWidth();

    for (int y = 0; y < height; y++) {
      double latitude = (1800d - y) / 1800d * 90d;
      int xSpread = (int) Math.round(Math.ceil(kmToPx(latitude, 5)));

      for (int x = 0; x < width; x++) {
        int ySpread = (int) Math.round(Math.ceil(kmToPx(5)));
        for (int ys = Math.max(0, y - ySpread); ys <= y + ySpread && ys < height; ys++) {
          if ((hollow.getRGB(x, y) | 0xFF000000) < 0xFFFFFFFF) {
            // Spread up, down, left and right.
            for (int xs = Math.max(0, x - xSpread); xs <= x + xSpread && xs < width; xs++) {
              filled.setRGB(xs, ys, 0xFF000000);
            }
          }
        }
      }
    }

    ImageIO.write(filled, "png", pngFile.toFile());
  }

  /**
   * The circumference of a parallel (line of longitude at a particular latitude) in kilometres.
   *
   * <p>Earth approximated as a sphere, which is sufficient for these bitmaps.
   */
  private double lengthParallelKm(double latitude) {
    return 2d * Math.PI * 6378.137 /* Earth radius */ * Math.cos(Math.toRadians(latitude));
  }

  /** Length of N kilometres in pixels, on a 7200×3600 pixel map. */
  private double kmToPx(double latitude, double nKm) {
    return nKm / (lengthParallelKm(latitude) / 7200d);
  }

  private double kmToPx(double nKm) {
    return nKm / (lengthParallelKm(0) / 7200d);
  }

  @Obsolete
  public void writeBitmap(String content, String outputFolder, String fileName) throws Exception {
    String svgFile = Paths.get(outputFolder, fileName + ".svg").toString();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(svgFile))) {
      writer.write(content);
      log.info("Successfully generated.");
    }

    log.info("{} is generated.", svgFile);
    log.info("Converting SVG to PNG");

    String pngFile = Paths.get(outputFolder, fileName + ".png").toString();
    Files.deleteIfExists(Paths.get(pngFile));
    Path filledPngFile = Files.createFile(Paths.get(pngFile));
    try (OutputStream pngOut = new FileOutputStream(filledPngFile.toFile())) {
      TranscoderInput svgImage = new TranscoderInput(svgFile);
      TranscoderOutput pngImage = new TranscoderOutput(pngOut);
      PNGTranscoder pngTranscoder = new PNGTranscoder();
      pngTranscoder.addTranscodingHint(ImageTranscoder.KEY_BACKGROUND_COLOR, Color.white);
      pngTranscoder.transcode(svgImage, pngImage);
    }
    log.info("{} is generated.", pngFile);
  }

  /** BitMapGenerator cw_state_poly feature /Users/Shared/Relocated Items/Security/data/sds-shp/ */
  public static void main(String[] args) throws Exception {

    String url =
        Strings.isNullOrEmpty(System.getProperty("url"))
            ? "jdbc:postgresql://127.0.0.1"
            : System.getProperty("url");
    String db = Strings.isNullOrEmpty(System.getProperty("db")) ? "eez" : System.getProperty("db");
    String user =
        Strings.isNullOrEmpty(System.getProperty("user")) ? "eez" : System.getProperty("user");
    String password =
        Strings.isNullOrEmpty(System.getProperty("password"))
            ? "eez"
            : System.getProperty("password");

    if (args.length != 3) {
      log.info("Error: args are incorrect!");
      log.info("Minimum three arguments required: layerName, AttrName, outputFolder");
      log.info("Example: BitMapGenerator cw_state_poly feature /data/sds-shp/");
      log.info(
          "It will generate a bitmap from cw_state_poly layer/table, and use attribute: feature to colourise polygons");
      log.info(
          "Complete command could be: java -Durl=jdbc:postgresql://127.0.0.1 -Ddb=sds -Duser=sds -Dpassword=sds -jar target/pipelines-BUILD_VERSION-shaded.jar au.org.ala.utils.BitMapGenerator cw_state_poly feature /data/sds-shp/");
    }

    String layer = args[0];
    String idName = args[1];
    String outputFolder = args[2];

    BitMapGenerator bmg = new BitMapGenerator(url, db, password, user);
    String[] svgs = bmg.generateSVG(layer, idName);
    // Use layer name as default file name.
    // bmg.writeBitmap(svg, outputFolder, layer);
    bmg.mergeTwoBitmaps(svgs, outputFolder, layer);
  }
}
