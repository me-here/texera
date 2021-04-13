package edu.uci.ics.texera.workflow.operators.source.scan.file.hdfs

import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpExecConfig

class HDFSFileScanSourceOpExecConfig(
    tag: OperatorIdentity,
    numWorkers: Int,
    totalBytes: Long,
    hdfsIP: String,
    filePath: String,
    schema: Schema,
    delimiter: Char,
    hasHeader: Boolean
) extends FileScanSourceOpExecConfig(tag, numWorkers, totalBytes) {

  override def getOpExec(startOffset: Long, endOffset: Long): IOperatorExecutor = {
    new HDFSFileScanSourceOpExec(
      hdfsIP,
      filePath,
      schema,
      delimiter,
      hasHeader,
      startOffset,
      endOffset
    )
  }

}
