package org.gbif.pipelines.core.config.factory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfigFactory {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  static {
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.findAndRegisterModules();
  }

  public static <T> T read(Path path, Class<T> clazz) {
    Function<Path, InputStream> absolute =
        p -> {
          try {
            return new FileInputStream(p.toFile());
          } catch (Exception ex) {
            String msg = "Properties with absolute p could not be read from " + path;
            throw new IllegalArgumentException(msg, ex);
          }
        };

    Function<Path, InputStream> resource =
        p -> Thread.currentThread().getContextClassLoader().getResourceAsStream(p.toString());

    Function<Path, InputStream> function = path.isAbsolute() ? absolute : resource;

    try (InputStream in = function.apply(path)) {
      // read properties from input stream
      return MAPPER.readValue(in, clazz);
    } catch (Exception ex) {
      String msg = "Properties with absolute path could not be read from " + path;
      throw new IllegalArgumentException(msg, ex);
    }
  }
}
