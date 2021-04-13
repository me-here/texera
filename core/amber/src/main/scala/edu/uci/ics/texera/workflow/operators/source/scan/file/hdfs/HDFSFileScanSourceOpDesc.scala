package edu.uci.ics.texera.workflow.operators.source.scan.file.hdfs

import java.io.InputStreamReader
import java.net.URI

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpDesc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jackson.map.annotate.JsonDeserialize

class HDFSFileScanSourceOpDesc extends FileScanSourceOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("HDFS IP address")
  @JsonPropertyDescription("IP address of the target Hadoop file system")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var hdfsIP: Option[String] = None

  fileTypeName = Option("HDFS")

  @JsonIgnore
  lazy val hdfs: FileSystem = FileSystem.get(new URI(hdfsIP.get), new Configuration())

  override def getOperatorExecutorConfig(path: String): OpExecConfig = {
    new HDFSFileScanSourceOpExecConfig(
      operatorIdentifier,
      Constants.defaultNumWorkers,
      hdfs.getFileStatus(new Path(path)).getLen,
      hdfsIP.get,
      path,
      inferSchema(),
      delimiter.get.charAt(0),
      hasHeader
    )
  }

  override def getFileInputStreamReader: InputStreamReader = {
    new InputStreamReader(hdfs.open(new Path(filePath.get)))
  }
}
