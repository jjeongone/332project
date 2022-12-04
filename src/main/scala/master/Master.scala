package cs332.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.io.OutputStream
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths}


import cs332.protos.sorting._

import java.nio.file.StandardOpenOption
import java.io.IOException


object Master {
    private val logger = Logger.getLogger(classOf[Master].getName)
    def main(args: Array[String]): Unit = { 
        val server = new Master(ExecutionContext.global)
        server.start()
        server.blockUntilShutdown()
    }

    private val port = 50051
}

class Master(executionContext: ExecutionContext) {self => 
    private [this] var server: Server = null
    private var tmpSharedSource = List[String]()
    private val workerCount = 2
    private val getWorkerPivotsLatch: CountDownLatch = new CountDownLatch(workerCount)
    private val SERVER_PATH = Paths.get("src/main/scala/master/sample")
    private def start(): Unit = {
        server = ServerBuilder.forPort(Master.port).addService(SorterGrpc.bindService(new SorterImpl, executionContext)).build.start
        Master.logger.info("Server started, listening on " + Master.port)
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
    
    private def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    private def modifyTmpSharedSource(): Unit = {
        this.synchronized{
            tmpSharedSource = tmpSharedSource.appended("OK")
            getWorkerPivotsLatch.countDown()
        }
    }

    private class SorterImpl extends SorterGrpc.Sorter{
        override def sayHello(req: HelloRequest) = {
            val reply = HelloReply(message = "Hello " + req.name)
            Future.successful(reply)
        }

        
        override def getWorkerPivots(responseObserver: StreamObserver[PivotResponse]): StreamObserver[PivotRequest] = {
            new StreamObserver[PivotRequest]() {
                var writer: OutputStream = null
                var status: Status = Status.IN_PROGRESS
                var sampleFileName = null

                override def onNext(req: PivotRequest): Unit = {
                    if (sampleFileName == null) {
                        writer = getFilePath(req)   
                    }

                    writeFile(writer, req.file.get.content.toString("UTF-8"))
                }    


                override def onError(throwable: Throwable): Unit = {
                    status = Status.FAILED
                    this.onCompleted()
                }

                override def onCompleted(): Unit = {
                    closeFile(writer)

                    modifyTmpSharedSource() // To be replaced by the sampling method
                    Master.logger.info("Waiting for others...")
                    getWorkerPivotsLatch.await()
                    
                    System.out.println(tmpSharedSource)

                    if (Status.IN_PROGRESS.equals(status)) {
                        status = Status.SUCCESS
                    }
                    val reply = PivotResponse(status = status)

                    responseObserver.onNext(reply)
                    responseObserver.onCompleted()
                }
            }
        }

        private def getFilePath(req: PivotRequest): OutputStream = {
            var fileName = req.metadata.get.fileName + "." + req.metadata.get.fileType
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

}