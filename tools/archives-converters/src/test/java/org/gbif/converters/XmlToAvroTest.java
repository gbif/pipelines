package org.gbif.converters;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.xml.SAXUtils;

import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class XmlToAvroTest {

  @Test
  public void testTest() throws Exception {

    URL url = new URL("https://rs.gbif.org/extension/dwc/measurements_or_facts.xml");

    // Read extension
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    Extension ext = factory.build(url.openStream(), url, false);

    List<Schema.Field> fields = new ArrayList<>(ext.getProperties().size() + 1);
    // Add gbifID
    fields.add(createSchemaField("gbifId", "GBIF OccurrenceId"));
    // Add RAW fields
    ext.getProperties().stream()
        .map(p -> createSchemaField("v_" + p.getName().toLowerCase(), p.getQualname()))
        .forEach(fields::add);
    // Add fields
    ext.getProperties().stream()
        .map(p -> createSchemaField(p.getName().toLowerCase(), p.getQualname()))
        .forEach(fields::add);

    String schema =
        Schema.createRecord(
                ext.getName() + "Table",
                "Avro Schema of Hive Table for " + ext.getName(),
                "org.gbif.pipelines.io.avro",
                false,
                fields)
            .toString(true);

    System.out.println(schema);
  }

  private Schema.Field createSchemaField(String name, String doc) {
    List<Schema> optionalString = new ArrayList<>();
    optionalString.add(Schema.create(Schema.Type.NULL));
    optionalString.add(Schema.create(Schema.Type.STRING));

    return new Schema.Field(name, Schema.createUnion(optionalString), doc, "null");
  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
