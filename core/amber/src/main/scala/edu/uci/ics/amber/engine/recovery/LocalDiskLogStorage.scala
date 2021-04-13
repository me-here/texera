package edu.uci.ics.amber.engine.recovery

import java.io.{DataInputStream, DataOutputStream, IOException, InputStream, OutputStream}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

class LocalDiskLogStorage[T](logName: String) extends FileLogStorage[T] {

  private lazy val path = Paths.get(s"./logs/$logName.logfile")

  override def getInputStream: DataInputStream = new DataInputStream(Files.newInputStream(path))

  override def getOutputStream: DataOutputStream = new DataOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND))

  override def fileExists: Boolean = Files.exists(path)

  override def createDirectories(): Unit = Files.createDirectories(path.getParent)

  override def deleteFile(): Unit = Files.delete(path)
}