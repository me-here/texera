package edu.uci.ics.amber.engine.recovery

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.file.Files

import com.twitter.chill.akka.AkkaSerializer
import edu.uci.ics.amber.engine.common.ambermessage.{ControlLogPayload, DPCursor, FromSender, LogRecord, LogWriterPayload, UpdateStepCursor}
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity
import edu.uci.ics.amber.engine.recovery.FileLogStorage.{ByteArrayReader, ByteArrayWriter, VirtualIdentityMapping, globalSerializer}
import edu.uci.ics.amber.error.ErrorUtils.safely
import org.apache.hadoop.fs.Syncable

import scala.collection.mutable

object FileLogStorage {
  val globalSerializer = new AkkaSerializer(null)

  case class VirtualIdentityMapping(vid: VirtualIdentity, id: Int)

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
  private var stepCursor:Long = 0L

  override def getStepCursor: Long = {
    if(loadedLogs.isEmpty){
      getLogs
    }
    stepCursor
  }

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
            case cursor: DPCursor =>
              stepCursor = cursor.idx
              loadedLogs.append(cursor)
            case VirtualIdentityMapping(vid, id) =>
              mapping(id) = vid
              loadedLogs.append(FromSender(vid))
            case vidRef: java.lang.Integer =>
              loadedLogs.append(FromSender(mapping(vidRef)))
            case ctrl: ControlLogPayload =>
              loadedLogs.append(ctrl)
            case payload: UpdateStepCursor =>
              stepCursor = payload.cursor
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

  override def write(record: LogRecord): Unit = {
    record match{
      case FromSender(virtualId) =>
        if (vidMapping.contains(virtualId)) {
          output.write(globalSerializer.toBinary(java.lang.Integer.valueOf(vidMapping(virtualId))))
        } else {
          vidMapping(virtualId) = count
          output.write(globalSerializer.toBinary(VirtualIdentityMapping(virtualId, count)))
          count += 1
        }
      case _ =>
        output.write(globalSerializer.toBinary(record))
    }
  }

  override def commit(): Unit = {
    output.flush()
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
