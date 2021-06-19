package edu.uci.ics.amber.engine.architecture.worker
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.flight.example.InMemoryStore
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables
class PythonRPCServer(portNumber: Int) extends Runnable with AutoCloseable {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);
  val location: Location = Location.forGrpcInsecure("localhost", portNumber)
  val mem: InMemoryStore = new InMemoryStore(allocator, location)
  val server: FlightServer = FlightServer.builder(allocator, location, mem).build()

  override def run(): Unit = {
    server.start()
  }

  @throws[Exception]
  override def close(): Unit = {
    AutoCloseables.close(mem, server, allocator)
  }
}
