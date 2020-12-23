package au.org.ala.pipelines.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.MDC;

@Slf4j
public class VersionInfo {

  public static void main(String[] args) {
    VersionInfo.print();
  }

  public static void print() {
    try {
      MDC.put("step", "VERSION_INFO");
      InputStream is = VersionInfo.class.getResourceAsStream("/git.properties");
      BufferedReader streamReader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      JSONParser parser = new JSONParser();
      JSONObject info = (JSONObject) parser.parse(streamReader);
      log.info("Commit URL: https://github.com/gbif/pipelines/commit/" + info.get("git.commit.id"));
      for (Object entry : info.entrySet()) {
        log.info(entry.toString());
      }
      log.debug("Details: " + info.toJSONString());
    } catch (Exception e) {
      log.warn("Cannot load git version information!");
    }
  }
}
