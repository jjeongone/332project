package cs332.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.io.OutputStream
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths}

import cs332.protos.sorting._
import cs332.common.Util.getIPaddress
import java.nio.file.StandardOpenOption
import java.io.IOException

object Master {
    private val logger = Logger.getLogger(classOf[Master].getName)
    def main(args: Array[String]): Unit = {
        val workerCount = args.headOption
        if (workerCount.isEmpty) {
            System.out.println("Worker Count is not given")
        }
        else {
            val server = new Master(ExecutionContext.global, workerCount.get.toInt)
            server.start()
            server.printEndpoint()  
            server.blockUntilShutdown()
        }
    }

    private val port = 50052
}

class Master(executionContext: ExecutionContext, workerCount: Int) {self => 
    private [this] var server: Server = null
    private val workerLatch: CountDownLatch = new CountDownLatch(workerCount)
    private var workers: List[String] = List()
    private var workerDone: List[(String, Pivot)] = List()
    private var workerPivots: List[Worker] = List()

    private def start(): Unit = {
        server = ServerBuilder.forPort(Master.port).addService(SorterGrpc.bindService(new SorterImpl, executionContext)).build.start
        Master.logger.info("Server has client Count of " + self.workerCount)
        Master.logger.info("Server started, listening on " + Master.port)
        sys.addShutdownHook {
            System.out.println("*** shutting down gRPC server since JVM is shutting down")
            self.stop()
            System.out.println("*** server shut down")
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

    private def printEndpoint(): Unit = {
        System.out.println(getIPaddress + ":" + Master.port)
    }

    private def addWorker(address: String): Unit = {
        this.synchronized{
            workers = workers.appended(address)
            if (workers.length == workerCount) {
                printWorkers()
            }
            workerLatch.countDown()
        }
    }

    private def printWorkers(): Unit = {
        System.out.println(workers.mkString(", "))
    }

    private def doneWorker(address: String, pivot: Pivot): Unit = {
        this.synchronized{
            workerDone = workerDone.appended((address, pivot))
            if (workerDone.length == workerCount) {
                // pass workerDone and workers and validation
                self.stop()
            }
        }
    }

    private class SorterImpl extends SorterGrpc.Sorter{
        private val SERVER_PATH = Paths.get(("src/main/scala/master/output"))

        override def registerWorker(req: RegisterRequest) = {
            var registerSuccess = true
            if (workers.length < workerCount) {
                addWorker(req.address)
            }
            else {
                registerSuccess = false
            }
            workerLatch.await()
            val reply = RegisterResponse(success = registerSuccess)
            Future.successful(reply)
        }

        override def getWorkerPivots(responseObserver: StreamObserver[PivotResponse]): StreamObserver[PivotRequest] = {
            new StreamObserver[PivotRequest]() {
                var writer: OutputStream = null
                var status: Status = Status.IN_PROGRESS

                override def onNext(req: PivotRequest): Unit = {
                    req.info match {
                        case PivotRequest.Info.Metadata(metadata) => {
                            writer = getFilePath(req)
                        }
                        case PivotRequest.Info.File(file) => {
                            writeFile(writer, req.info.file.get.content.toString("UTF-8"))
                        }
                        case PivotRequest.Info.Empty => {}
                    }
                }    


                override def onError(throwable: Throwable): Unit = {
                    status = Status.FAILED
                    this.onCompleted()
                }

                override def onCompleted(): Unit = {
                    closeFile(writer)
                    if (Status.IN_PROGRESS.equals(status)) {
                        status = Status.SUCCESS
                    }
                    val reply = PivotResponse(status = status, worker = workerPivots)
                    responseObserver.onNext(reply)
                    responseObserver.onCompleted()
                }
            }
        }

        override def mergeDone(req: DoneRequest) = {
            doneWorker(req.address, req.pivot.get)
            val reply = DoneResponse(ok = true)
            Future.successful(reply)
        }

        private def getFilePath(req: PivotRequest): OutputStream = {
            var fileName = req.info.metadata.get.name + "." + req.info.metadata.get.fileType
            Files.newOutputStream(SERVER_PATH.resolve(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        }

        private def writeFile(writer: OutputStream, content: String) = {
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