package worker.client

import cs332.common.Util.getIPaddress
import cs332.file.protos.shuffling.{RegisterRequest, ShuffleRequest, ShuffleResponse, ShufflerGrpc}
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
    new WorkerFileClient(channel, blockingStub, newStub, workerFilePort)
  }

  def main(ipAddress: String, port: Int): Unit = {
    val client = WorkerFileClient(ipAddress, port)
    System.out.println("Connecting to " + ipAddress + ":" + port)

    try {
      val succeedRegistration = client.register()
      if (!succeedRegistration) {
        client.shutdown()
      } else {
        client.shuffle()
      }
    } finally {
      client.shutdown()
    }
  }

  private val workerFilePort = 8081
}

class WorkerFileClient private (private val channel: ManagedChannel, private val blockingStub: ShufflerBlockingStub, private val newStub: ShufflerStub, private val myPort: Int) {
  private [this] val logger = Logger.getLogger(classOf[WorkerFileClient].getName)
  private val SERVER_PATH = Paths.get("src/main/scala/worker/input")
  private val myAddress = getIPaddress

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
  }

  def register(): Boolean = {
    val endpoint = myAddress + ":" + myPort
    logger.info("REGISTER : current endpoint " + endpoint)
    var successToConnect = false
    while (!successToConnect) {
      val req = RegisterRequest(address = endpoint)
      try {
        logger.info("REGISTER : try to connecting...")
        val res = blockingStub.registerFileServer(req)
        if (res.status) {
          logger.info("REGISTER : file server registration success")
          successToConnect = true
        } else {
          logger.info("REGISTER : file server registration fail")
        }
      } catch {
        case e: StatusRuntimeException =>
          logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
      }
      Thread.sleep(1000)
    }
    successToConnect
  }

  def shuffle(): Unit = {
    var writer: OutputStream = null
    var sampleFileName = null

    logger.info("Will try to get files from other workers")
    val request = ShuffleRequest(address = "localhost", count = "2")
    try {
      val responses = blockingStub.shuffling(request)
      logger.info("responses" + responses)
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
    val fileName = req.metadata.get.workerOrder + "." + req.metadata.get.fileName + "." + req.metadata.get.fileType
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
