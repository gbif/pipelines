package org.gbif.validator.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import java.io.IOException;
import java.net.URI;
import lombok.Data;
import lombok.SneakyThrows;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.validator.ws.file.DownloadFileManager;
import org.gbif.validator.ws.file.UploadFileManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.multipart.support.MultipartFilter;

/** Ws Validation application Spring configuration. */
@Configuration
@Data
@EnableAsync
public class ValidatorWsConfiguration {

  public static final String FILE_POST_PARAM_NAME = "file";

  /** Supported XSD schemas. */
  @Data
  public static class XmlSchemaLocations {
    private String eml;
    private String emlGbifProfile;
    private String[] dwcMeta;
  }

  @Bean
  @ConfigurationProperties("schemas")
  public XmlSchemaLocations schemaLocations() {
    return new XmlSchemaLocations();
  }

  @Bean
  @SneakyThrows
  public SchemaValidatorFactory schemaValidatorFactory(XmlSchemaLocations xmlSchemaLocations) {
    SchemaValidatorFactory schemaValidatorFactory =
        new SchemaValidatorFactory(xmlSchemaLocations.getDwcMeta());
    schemaValidatorFactory.load(new URI(xmlSchemaLocations.getEml()));
    schemaValidatorFactory.load(new URI(xmlSchemaLocations.getEmlGbifProfile()));
    return schemaValidatorFactory;
  }

  @Bean
  public UploadFileManager uploadedFileManager(
      @Value("${upload.workingDirectory}") String uploadWorkingDirectory,
      @Value("${storePath}") String storePath,
      DownloadFileManager downloadFileManager)
      throws IOException {
    return new UploadFileManager(uploadWorkingDirectory, storePath, downloadFileManager);
  }

  @Bean("multipartResolver")
  public CommonsMultipartResolver multipartResolver(
      @Value("${upload.maxUploadSize}") Long maxUploadSize) {
    CommonsMultipartResolver multipart = new CommonsMultipartResolver();
    multipart.setMaxUploadSize(maxUploadSize);
    return multipart;
  }

  @Bean
  @Order(0)
  public MultipartFilter multipartFilter() {
    MultipartFilter multipartFilter = new MultipartFilter();
    multipartFilter.setMultipartResolverBeanName("multipartResolver");
    return multipartFilter;
  }

  /** Configure the Jackson ObjectMapper adding a custom JsonFilter for errors. */
  @Configuration
  public static class FilterConfiguration {

    public FilterConfiguration(ObjectMapper objectMapper) {
      // This filter only keeps a minimum of fields in a SaxParserException
      FilterProvider simpleFilterProvider =
          new SimpleFilterProvider()
              .addFilter(
                  "stackTrace",
                  SimpleBeanPropertyFilter.filterOutAllExcept(
                      "lineNumber", "columnNumber", "message"));
      objectMapper.setFilterProvider(simpleFilterProvider);
    }
  }
}
