package edu.uci.ics.texera.workflow.operators.source.scan.file.local

import java.io.{FileReader, InputStreamReader}

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpDesc

class LocalFileScanSourceOpDesc extends FileScanSourceOpDesc {

  override def getOperatorExecutorConfig(path: String): OpExecConfig = {
    new LocalFileScanSourceOpExecConfig(
      operatorIdentifier,
      Constants.defaultNumWorkers,
      path,
      inferSchema(),
      delimiter.get.charAt(0),
      hasHeader
    )
  }

  override def getFileInputStreamReader: InputStreamReader = new FileReader(filePath.get)
}
