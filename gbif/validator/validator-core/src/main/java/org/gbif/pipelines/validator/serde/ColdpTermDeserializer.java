package org.gbif.pipelines.validator.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import life.catalogue.coldp.ColdpTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

public class ColdpTermDeserializer extends JsonDeserializer<Term> {

  private static final TermFactory factory = TermFactory.instance();

  static {
    factory.registerTermEnum(ColdpTerm.class);
  }

  @Override
  public Term deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
      return factory.findTerm(jp.getText());
    }
    throw JsonMappingException.from(jp, "Expected JSON String");
  }
}
