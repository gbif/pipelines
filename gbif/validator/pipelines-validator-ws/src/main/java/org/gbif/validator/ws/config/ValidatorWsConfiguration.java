package org.gbif.validator.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import lombok.Data;
import lombok.SneakyThrows;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.gbif.pipelines.validator.ws.ChecklistbankWsClient;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.file.DownloadFileManager;
import org.gbif.validator.ws.file.FileStoreManager;
import org.gbif.validator.ws.serde.ValidationMixin;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

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
  public FileStoreManager uploadedFileManager(
      @Value("${spring.servlet.multipart.location}") String uploadWorkingDirectory,
      @Value("${storePath}") String storePath,
      DownloadFileManager downloadFileManager)
      throws IOException {
    return new FileStoreManager(uploadWorkingDirectory, storePath, downloadFileManager);
  }

  @Bean("multipartResolver")
  public StandardServletMultipartResolver multipartResolver() {
    return new StandardServletMultipartResolver();
  }

  @Primary
  @Bean
  public ObjectMapper registryObjectMapper() {
    ObjectMapper objectMapper = JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.addMixIn(Validation.class, ValidationMixin.class);
    return objectMapper;
  }

  @Bean
  public ChecklistbankWsClient checklistbankWsClient(
      @Value("${clb.api.url}") String clbApiUrl,
      @Value("${clb.api.user}") String clbApiUser,
      @Value("${clb.api.password}") String clbApiPassword) {
    return new ClientBuilder()
        .withUrl(clbApiUrl)
        .withCredentials(clbApiUser, clbApiPassword)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getDefaultObjectMapper())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(ChecklistbankWsClient.class);
  }

  @Bean
  public ChecklistValidator checklistValidator(ChecklistbankWsClient checklistbankWsClient) {
    // the callback is not needed in the ws, only in the cli
    return new ChecklistValidator(checklistbankWsClient, null);
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
