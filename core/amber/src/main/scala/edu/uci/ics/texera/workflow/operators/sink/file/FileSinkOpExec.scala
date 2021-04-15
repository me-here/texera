package edu.uci.ics.texera.workflow.operators.sink.file

import java.io.{File, FileWriter, IOException}
import java.nio.file.{Files, Path}

import com.github.tototoshi.csv.CSVWriter
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.web.WebUtils
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.jooq.types.UInteger

class FileSinkOpExec(
    val fileName: String,
    val fileType: ResultFileType,
    val userId: UInteger
) extends IOperatorExecutor {

  var headerInitialized: Boolean = false;
  var csvWriter: CSVWriter = null

  override def open(): Unit = {
    // currently assume server and client are on the same machine so file could be accessed locally
    // TODO change the file saving so that server and client could access the file on different machine
    val file: File = WebUtils.locateResultFile(userId.toString, fileName + fileType.fileSuffix)
    val directory: Path = file.toPath.getParent

    try {
      if (Files.notExists(directory)) Files.createDirectories(directory)

      fileType match {
        case ResultFileType.EXCEL => csvWriter = new CSVWriter(new FileWriter(file))
        case _                    => throw new RuntimeException("Unsupported file download type " + fileType.getName)
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException("fail to create file", e)
    }

    // TODO generate header here when schema are known during open stage later
  }

  override def close(): Unit = {
    if (csvWriter != null) csvWriter.close()
  }

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): scala.Iterator[ITuple] = {
    tuple match {
      case Left(t) =>
        fileType match {
          case ResultFileType.EXCEL => {
            // TODO only takes Tuple now. Generate header during open() method After separating the schema and tuple
            createHeaderIfNotExist(t.asInstanceOf[Tuple].getSchema)
            csvWriter.writeRow(t.toSeq)
          }
        }
        Iterator()
      case Right(_) =>
        Iterator()
    }
  }

  def createHeaderIfNotExist(schema: Schema): Unit = {
    if (headerInitialized) return
    csvWriter.writeRow(schema.getAttributeNames.toArray)
    headerInitialized = true
  }
}
