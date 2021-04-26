package edu.uci.ics.amber.engine.recovery

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.file.Files

import com.twitter.chill.akka.AkkaSerializer
import edu.uci.ics.amber.engine.common.ambermessage.{
  DPCursor,
  DataBatchSequence,
  FromSender,
  LogRecord,
  LogWriterPayload,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity
import edu.uci.ics.amber.engine.recovery.FileLogStorage.{
  ByteArrayReader,
  ByteArrayWriter,
  SenderWithSeqNum,
  VirtualIdentityMapping,
  globalSerializer
}
import edu.uci.ics.amber.error.ErrorUtils.safely
import org.apache.hadoop.fs.Syncable

import scala.collection.mutable

object FileLogStorage {
  val globalSerializer = new AkkaSerializer(null)

  case class VirtualIdentityMapping(vid: VirtualIdentity, id: Int, seq: Long)
  case class SenderWithSeqNum(id: Int, seq: Long)

  class ByteArrayWriter(outputStream: DataOutputStream) {

    def write(content: Array[Byte]): Unit = {
      outputStream.writeInt(content.length)
      outputStream.write(content)
    }

    def flush(): Unit = {
      outputStream match {
        case syncable: Syncable =>
          syncable.hsync()
        case _ =>
          outputStream.flush()
      }
    }

    def close(): Unit = {
      outputStream.close()
    }
  }

  class ByteArrayReader(inputStream: DataInputStream) {

    def read(): Array[Byte] = {
      val length = inputStream.readInt()
      inputStream.readNBytes(length)
    }

    def close(): Unit = {
      inputStream.close()
    }

    def isAvailable: Boolean = {
      inputStream.available() >= 4
    }
  }
}

abstract class FileLogStorage(logName: String) extends LogStorage(logName) {

  def getInputStream: DataInputStream

  def getOutputStream: DataOutputStream

  def fileExists: Boolean

  def createDirectories(): Unit

  def deleteFile(): Unit

  private lazy val output = new ByteArrayWriter(getOutputStream)

  private val vidMapping = mutable.HashMap[VirtualIdentity, Int]()
  private var count = 0
  private val loadedLogs = mutable.ArrayBuffer.empty[LogRecord]

  override def getLogs: Iterable[LogRecord] = {
    createDirectories()
    // read file
    if (!fileExists) {
      Iterable.empty
    } else {
      if (loadedLogs.nonEmpty) {
        return loadedLogs
      }
      val input = new ByteArrayReader(getInputStream)
      val mapping = mutable.HashMap[Int, VirtualIdentity]()
      while (input.isAvailable) {
        try {
          val binary = input.read()
          val message = globalSerializer.fromBinary(binary)
          message match {
            case cursor: java.lang.Long =>
              loadedLogs.append(DPCursor(cursor))
            case VirtualIdentityMapping(vid, id, seq) =>
              mapping(id) = vid
              loadedLogs.append(FromSender(vid, seq))
            case SenderWithSeqNum(vidRef, seq) =>
              loadedLogs.append(FromSender(mapping(vidRef), seq))
            case ctrl: WorkflowControlMessage =>
              loadedLogs.append(ctrl)
            case other =>
              throw new RuntimeException(
                "cannot deserialize log: " + (binary.map(_.toChar)).mkString
              )
          }
        } catch {
          case e: Exception =>
            input.close()
            throw e
        }
      }
      input.close()
      loadedLogs
    }
  }

  override def writeControlLogRecord(record: WorkflowControlMessage): Unit =
    output.write(globalSerializer.toBinary(record))

  override def writeDataLogRecord(from: VirtualIdentity, seq: Long): Unit = {
    if (vidMapping.contains(from)) {
      output.write(globalSerializer.toBinary(SenderWithSeqNum(vidMapping(from), seq)))
    } else {
      vidMapping(from) = count
      output.write(globalSerializer.toBinary(VirtualIdentityMapping(from, count, seq)))
      count += 1
    }
  }

  override def writeDPLogRecord(cursor: Long): Unit =
    output.write(globalSerializer.toBinary(java.lang.Long.valueOf(cursor)))

  override def commit(): Unit = {
    output.flush()
  }

  def persistRecord(elem: LogWriterPayload): Unit = {
    output.write(globalSerializer.toBinary(elem))
  }

  override def clear(): Unit = {
    if (fileExists) {
      deleteFile()
    }
  }

  override def release(): Unit = {
    try {
      output.close()
    } catch {
      case e: Exception =>
        println("error occurs when closing the output: " + e.getMessage)
    }
  }

}
