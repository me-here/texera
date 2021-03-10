package edu.uci.ics.texera.workflow.operators.sink.file

import java.io.{FileWriter, IOException}
import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import java.util.Date

import com.github.tototoshi.csv.CSVWriter
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import org.jooq.types.UInteger

import scala.collection.mutable

class FileSinkOpExec(
    val fileName: String,
    val fileType: ResultFileType,
    val userId: UInteger
) extends IOperatorExecutor {

  // TODO change path
  val BaseDirectory: Path = Path.of("C:\\Users\\SuperTaunt\\Desktop")
  var csvWriter: CSVWriter = null

  override def open(): Unit = {
    // currently assume server and client are on the same machine so file could be accessed locally
    // TODO change the file saving so that server and client could access the file on different machine
    val directory: Path = BaseDirectory.resolve(userId.toString)
    val file = directory.resolve(fileName + fileType.fileSuffix).toFile

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

    // TODO write header
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
            // TODO get header
            csvWriter.writeRow(t.toSeq)
          }
        }
        Iterator()
      case Right(_) =>
        Iterator()
    }
  }

}
