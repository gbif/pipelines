package org.gbif.pipelines.validator.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.gbif.dwc.terms.Term;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

public class ObjectMapperUtils {

  public static ObjectMapper createObjectMapperWithColDPSupport() {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Term.class, new ColdpTermDeserializer());

    return JacksonJsonObjectMapperProvider.getDefaultObjectMapper().registerModule(module);
  }
}
