package org.gbif.pipelines.core.interpreters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Multimedia;

import org.junit.Test;

public class ExtensionInterpreterTest {

  @Test
  public void teqwr() {

    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DcTerm.title.qualifiedName(), "Taidelalde!");

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DcTerm.title.qualifiedName(), "123123123123!");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MULTIMEDIA.getRowType(), Arrays.asList(ext1, ext2));

    ExtendedRecord record = ExtendedRecord.newBuilder()
        .setId("id")
        .setExtensions(ext)
        .build();

    System.out.println(record);

    ///////////////////////////////////////
    List<Multimedia> multimedia2 = ExtensionInterpretation.extenstion(Extension.MULTIMEDIA)
        .from(record)
        .to(Multimedia::new)
        .map(DcTerm.title, Multimedia::setTitle)
        .convert()
        .getList();

    System.out.println(multimedia2);
  }

}
