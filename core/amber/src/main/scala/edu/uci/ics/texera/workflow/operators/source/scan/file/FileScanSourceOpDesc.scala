package edu.uci.ics.texera.workflow.operators.source.scan.file

import java.io.{BufferedReader, IOException, InputStreamReader}

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import org.codehaus.jackson.map.annotate.JsonDeserialize

import scala.collection.JavaConverters._

abstract class FileScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var delimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the file contains a header line")
  var hasHeader: Boolean = true

  def getOperatorExecutorConfig(path: String): OpExecConfig

  def getFileInputStreamReader: InputStreamReader

  @throws[IOException]
  override def operatorExecutor: OpExecConfig = {
    // fill in default values
    if (delimiter.get.isEmpty)
      delimiter = Option(",")

    filePath match {
      case Some(path) =>
        getOperatorExecutorConfig(path)
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (delimiter.isEmpty) return null
    val reader = new BufferedReader(getFileInputStreamReader)
    var headerLine: Option[String] = None
    if (hasHeader) {
      headerLine = Some(reader.readLine())
    }
    val headers: Array[String] = headerLine.get.split(delimiter.get)

    // TODO: real CSV may contain multi-line values. Need to handle multi-line values correctly.

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader
        .lines()
        .iterator
        .asScala
        .take(INFER_READ_LIMIT)
        .map(line => line.split(delimiter.get).asInstanceOf[Array[Object]])
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema.newBuilder
      .add(
        if (hasHeader)
          headers.indices
            .map((i: Int) => new Attribute(headers.apply(i), attributeTypeList.apply(i)))
            .asJava
        else
          headers.indices
            .map((i: Int) => new Attribute("column-" + (i + 1), attributeTypeList.apply(i)))
            .asJava
      )
      .build
  }
}
