package au.org.ala.pipelines.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import au.org.ala.utils.ALAFsUtils;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapperBuilder;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Builder(buildMethodName = "create")
public class DwcaMetaXml {

  private static final String META =
      "<?xml version=\"1.0\"?>\n"
          + "<archive metadata=\"eml.xml\" xmlns=\"http://rs.tdwg.org/dwc/text/\">\n"
          + "  <core encoding=\"UTF-8\" linesTerminatedBy=\"\\r\\n\" fieldsTerminatedBy=\"\\t\" fieldsEnclosedBy=\"&quot;\" ignoreHeaderLines=\"0\" rowType=\"http://rs.tdwg.org/dwc/terms/Occurrence\">\n"
          + "    <files>\n"
          + "      <location>occurrence.tsv</location>\n"
          + "    </files>\n"
          + "    <id index=\"0\"/>\n"
          + "    <#list coreTerms as coreTerm>\n"
          + "    <field index=\"${coreTerm?counter - 1}\" term=\"${coreTerm}\"/>\n"
          + "    </#list>\n"
          + "  </core>\n"
          + "  <#list multimediaTerms>\n"
          + "  <extension encoding=\"UTF-8\" linesTerminatedBy=\"\\r\\n\" fieldsTerminatedBy=\"\\t\" fieldsEnclosedBy=\"&quot;\" ignoreHeaderLines=\"0\" rowType=\"http://rs.gbif.org/terms/1.0/Multimedia\">\n"
          + "    <files>\n"
          + "      <location>image.tsv</location>\n"
          + "    </files>\n"
          + "    <coreid index=\"0\"/>\n"
          + "    <#items as multimediaTerm>\n"
          + "    <field index=\"${multimediaTerm?counter - 1}\" term=\"${multimediaTerm}\"/>\n"
          + "    </#items>\n"
          + "  </extension>\n"
          + "  </#list>\n"
          + "</archive>";

  private final List<String> coreTerms;
  private final List<String> multimediaTerms;
  private final String pathToWrite;
  private final String hdfsSiteConfig;
  private final String coreSiteConfig;

  @SneakyThrows
  public void write() {
    Template temp = new Template("meta", new StringReader(META), createConfig());
    try (Writer out = getWriter()) {
      temp.process(TemplatePojo.create(coreTerms, multimediaTerms), out);
    }
  }

  private Writer getWriter() throws IOException {
    if (hdfsSiteConfig == null || hdfsSiteConfig.isEmpty()) {
      FileUtils.forceMkdir(new File(pathToWrite).getParentFile());
      return new OutputStreamWriter(new FileOutputStream(pathToWrite));
    } else {
      FileSystem fs =
          FsUtils.getFileSystem(HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig), pathToWrite);
      return new OutputStreamWriter(ALAFsUtils.openOutputStream(fs, pathToWrite));
    }
  }

  /** Create Freemarker config */
  private Configuration createConfig() {
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
    cfg.setDefaultEncoding(UTF_8.toString());
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    cfg.setLogTemplateExceptions(false);
    cfg.setWrapUncheckedExceptions(true);
    cfg.setFallbackOnNullLoopVariable(false);
    cfg.setTabSize(1);

    DefaultObjectWrapperBuilder owb = new DefaultObjectWrapperBuilder(Configuration.VERSION_2_3_31);
    owb.setIterableSupport(true);
    cfg.setObjectWrapper(owb.build());

    return cfg;
  }

  @Getter
  @Data(staticConstructor = "create")
  public static class TemplatePojo {
    private final List<String> coreTerms;
    private final List<String> multimediaTerms;
  }
}
