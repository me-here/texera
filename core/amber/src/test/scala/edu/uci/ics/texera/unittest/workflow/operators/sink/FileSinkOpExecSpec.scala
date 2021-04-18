package edu.uci.ics.texera.unittest.workflow.operators.sink

import java.io.File

import com.github.tototoshi.csv.CSVReader
import edu.uci.ics.texera.web.WebUtils
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import edu.uci.ics.texera.workflow.operators.sink.file.{FileSinkOpExec, ResultFileType}
import org.jooq.types.UInteger
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FileSinkOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tuple: Tuple = Tuple
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .add(new Attribute("field4", AttributeType.LONG), Long.MaxValue)
    .build()
  var fileSinkOpExec: FileSinkOpExec = _
  var testFileName: String = "testFileName"
  var testFileType: ResultFileType = ResultFileType.EXCEL
  var testUid: UInteger = UInteger.valueOf(12345)

  before {
    fileSinkOpExec = new FileSinkOpExec(testFileName, testFileType, testUid)
  }
  after {
    val file: File =
      WebUtils.locateResultFile(testUid.toString, testFileName + testFileType.fileSuffix)
    if (file.exists()) file.delete()
  }

  it should " file should created when calling open() function" in {
    fileSinkOpExec.open()
    fileSinkOpExec.close()

    val file: File =
      WebUtils.locateResultFile(testUid.toString, testFileName + testFileType.fileSuffix)
    assert(file.exists)
    assert(file.canRead)
  }

  it should "process Tuple and save results to file" in {
    fileSinkOpExec.open()
    fileSinkOpExec.processTuple(Left(tuple), null)
    fileSinkOpExec.close()

    val file: File =
      WebUtils.locateResultFile(testUid.toString, testFileName + testFileType.fileSuffix)
    val reader: CSVReader = CSVReader.open(file)
    val result: List[List[String]] = reader.all()

    assert(result.length == 2)
    assert(result(0).length == 4)
    assert(result(1).length == 4)
    assert(result(0).sameElements(tuple.getSchema.getAttributeNames.toArray))
    assert(result(1).sameElements(tuple.getFields.stream().map(e => e.toString).toArray))

    reader.close()
  }
}
