package edu.uci.ics.texera.workflow.operators.source.scan.file.local

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import java.io.File

import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpExecConfig

class LocalFileScanSourceOpExecConfig(
    tag: OperatorIdentity,
    numWorkers: Int,
    filePath: String,
    schema: Schema,
    delimiter: Char,
    hasHeader: Boolean
) extends FileScanSourceOpExecConfig(tag, numWorkers, new File(filePath).length()) {

  override def getOpExec(startOffset: Long, endOffset: Long): IOperatorExecutor = {
    new LocalFileScanSourceOpExec(
      filePath,
      schema,
      delimiter,
      hasHeader,
      startOffset,
      endOffset
    )
  }
}
