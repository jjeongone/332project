package worker.server

import cs332.file.protos.shuffling._
import io.grpc.stub.StreamObserver
import io.grpc.{Server, ServerBuilder}

import java.util.logging.{Level, Logger}
import java.nio.file.{Files, Paths}
import java.io.InputStream
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}


object WorkerFileServer {
  private val logger = Logger.getLogger(classOf[WorkerFileServer].getName)

  def main(workerOrder: Int): Unit = {
    val server = new WorkerFileServer(ExecutionContext.global, workerOrder)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50053
}

class WorkerFileServer(executionContext: ExecutionContext, workerOrder: Int) {self =>
  private [this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(WorkerFileServer.port).addService(ShufflerGrpc.bindService(new ShufflerImpl, executionContext)).build.start
    WorkerFileServer.logger.info("Worker file server started, listening on " + WorkerFileServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown():Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class ShufflerImpl extends ShufflerGrpc.Shuffler {
    override def registerFileServer(request: RegisterRequest): Future[RegisterResponse] = {
      val res = RegisterResponse(status = true)
      WorkerFileServer.logger.info("Worker file server connection...")
      Future.successful(res)
    }
    override def shuffling(request: ShuffleRequest, responseObserver: StreamObserver[ShuffleResponse]): Unit = {
      WorkerFileServer.logger.info("Will try to shuffle files "  + " ...")
      val pathStrings = List("src/main/scala/worker/input/sample/sample0.1", "src/main/scala/worker/input/sample/sample1.1", "src/main/scala/worker/input/sample/sample2.1")
      pathStrings.foldLeft(())((acc, pathString) => {
        val path = Paths.get(pathString)

        val fileName = pathString.split('/').last
        val metadata = Metadata(fileName = fileName.split('.').head, fileType = fileName.split('.').last, workerOrder = workerOrder)

        val inputStream: InputStream = Files.newInputStream(path)
        var bytes = Array.ofDim[Byte](2000000)

        var size: Int = 0
        size = inputStream.read(bytes)
        while (size > 0) {
          val content = ByteString.copyFrom(bytes, 0, size)
          val file = FileMessage(content = content)
          val fileResponse = ShuffleResponse(metadata = Option(metadata), file = Option(file))
          responseObserver.onNext(fileResponse)
          size = inputStream.read(bytes)
        }
        inputStream.close()
      })
      responseObserver.onCompleted()
    }
  }
}
