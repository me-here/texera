package edu.uci.ics.amber.engine.recovery

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.nio.file.Files

import com.twitter.chill.akka.AkkaSerializer
import edu.uci.ics.amber.engine.recovery.FileLogStorage.{ByteArrayReader, ByteArrayWriter, globalSerializer}
import edu.uci.ics.amber.error.ErrorUtils.safely

import scala.collection.mutable

object FileLogStorage{
  val globalSerializer = new AkkaSerializer(null)

  class ByteArrayWriter(outputStream: DataOutputStream) {

    def writeAndFlush(content:Array[Byte]): Unit ={
      outputStream.writeInt(content.length)
      outputStream.write(content)
      outputStream.flush()
    }

    def close(): Unit ={
      outputStream.close()
    }
  }

  class ByteArrayReader(inputStream:DataInputStream) {

    def read():Array[Byte] ={
      val length = inputStream.readInt()
      inputStream.readNBytes(length)
    }

    def close(): Unit ={
      inputStream.close()
    }

    def isAvailable:Boolean = {
      inputStream.available() >= 4
    }
  }
}

abstract class FileLogStorage[T] extends LogStorage[T] {

  def getInputStream:DataInputStream

  def getOutputStream:DataOutputStream

  def fileExists:Boolean

  def createDirectories():Unit

  def deleteFile():Unit

  private lazy val output = new ByteArrayWriter(getOutputStream)

  override def load(): Iterable[T] = {
    createDirectories()
    // read file
    if (!fileExists) {
      return Iterable.empty
    }
    val input = new ByteArrayReader(getInputStream)
    val buf = mutable.ArrayBuffer.empty[T]
    while (input.isAvailable) {
      try {
        val binary = input.read()
        val message = globalSerializer.fromBinary(binary).asInstanceOf[T]
        buf.append(message)
      } catch {
        case e:Exception =>
          input.close()
          throw e
      }
    }
    input.close()
    buf
  }

  override def persistElement(elem: T): Unit = {
    try {
      val byteArray = globalSerializer.toBinary(elem.asInstanceOf[AnyRef])
      output.writeAndFlush(byteArray)
    } catch safely {
      case e:Throwable => throw e
    }
  }

  override def clear(): Unit = {
    if(fileExists){
      deleteFile()
    }
  }

  override def release(): Unit = {
    try{
      output.close()
    }catch{
      case e:Exception =>
        println("error occurs when closing the output: "+e.getMessage)
    }
  }

}
