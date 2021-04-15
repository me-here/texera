package edu.uci.ics.texera.unittest.workflow.operators.sink

import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.sink.file.{FileSinkOpDesc, ResultFileType}
import org.jooq.types.UInteger
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FileSinkOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.INTEGER),
    new Attribute("field3", AttributeType.BOOLEAN),
    new Attribute("field4", AttributeType.LONG)
  )
  var fileSinkOpDesc: FileSinkOpDesc = _

  before {
    fileSinkOpDesc = new FileSinkOpDesc()
  }

  it should "take in attribute names" in {

    val testFileName: String = "testFileName"
    val testFileType: ResultFileType = ResultFileType.EXCEL

    fileSinkOpDesc.fileName = testFileName
    fileSinkOpDesc.fileType = testFileType

    assert(testFileName.equals(fileSinkOpDesc.fileName))
    assert(testFileType.equals(fileSinkOpDesc.fileType))
  }

  it should "raise RuntimeException when user not login" in {

    val testFileName: String = "testFileName"
    val testFileType: ResultFileType = ResultFileType.EXCEL
    val context: WorkflowContext = new WorkflowContext()
    context.jobID = "abc"
    context.userID = Option.empty

    fileSinkOpDesc.fileName = testFileName
    fileSinkOpDesc.fileType = testFileType
    fileSinkOpDesc.context = context

    assertThrows[RuntimeException] {
      fileSinkOpDesc.operatorExecutor()
    }
  }

  it should "create executor successfully" in {

    val testFileName: String = "testFileName"
    val testFileType: ResultFileType = ResultFileType.EXCEL
    val context: WorkflowContext = new WorkflowContext()
    context.jobID = "abc"
    context.userID = Option.apply(UInteger.valueOf(12345))

    fileSinkOpDesc.fileName = testFileName
    fileSinkOpDesc.fileType = testFileType
    fileSinkOpDesc.context = context

    assert(null != fileSinkOpDesc.operatorExecutor())
  }

}
