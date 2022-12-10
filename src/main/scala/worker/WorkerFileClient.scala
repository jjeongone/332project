package worker.client

import cs332.file.protos.shuffling.{ShuffleRequest, ShuffleResponse, ShufflerGrpc}
import cs332.file.protos.shuffling.ShufflerGrpc.{ShufflerBlockingStub, ShufflerStub}

import java.util.logging.{Level, Logger}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import java.io.OutputStream
import java.util.concurrent.TimeUnit
import java.nio.file.{Files, Paths}
import java.nio.file.StandardOpenOption


object WorkerFileClient {
  def apply(host: String, port: Int): WorkerFileClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ShufflerGrpc.blockingStub(channel)
    val newStub = ShufflerGrpc.stub(channel)
    new WorkerFileClient(channel, blockingStub, newStub)
  }

  def main(ipAddress: String, port: Int): Unit = {
    val client = WorkerFileClient(ipAddress, port)
    try {
      client.shuffle()
    } finally {
      client.shutdown()
    }
  }
}

class WorkerFileClient private (private val channel: ManagedChannel, private val blockingStub: ShufflerBlockingStub, private val newStub: ShufflerStub) {
  private [this] val logger = Logger.getLogger(classOf[WorkerFileClient].getName)
  private val SERVER_PATH = Paths.get("src/main/scala/worker/sample")

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
  }

  def shuffle(): Unit = {
    var writer: OutputStream = null
    var sampleFileName = null

    logger.info("Will try to get files from other workers")
    val request = ShuffleRequest(address = "localhost", count = "2")
    try {
      val responses = blockingStub.shuffling(request)
      while(responses.hasNext) {
        val res = responses.next()
        if (sampleFileName == null) {
          writer = getFilePath(res)
        }
        logger.info("Writing file...")
        writeFile(writer, res.file.get.content.toString("UTF-8"))
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  private def getFilePath(req: ShuffleResponse): OutputStream = {
    val fileName = req.metadata.get.fileName + "." + req.metadata.get.fileType
    Files.newOutputStream(SERVER_PATH.resolve(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  private def writeFile(writer: OutputStream, content: String) = { // Check if race condition occurred itself
    writer.write(content.getBytes())
    writer.flush()
  }

  private def closeFile(writer: OutputStream) = {
    try {
      writer.close()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
