package cs332.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.io.OutputStream
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths}

import cs332.protos.sorting._
import cs332.common.Util.{getIPaddress, currentDirectory, makeSubdirectory}
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
    private val samplePath = "samples"
    private val SERVER_PATH = Paths.get(makeSubdirectory(currentDirectory, samplePath))

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

    private def addWorker(address: String): Int = {
        var workerOrder = -1
        this.synchronized{
            workerOrder = workers.length
            workers = workers.appended(address)
            if (workers.length == workerCount) {
                printWorkers()
            }
        }
        workerOrder
    }

    private def printWorkers(): Unit = {
        System.out.println(workers.mkString(", "))
    }

    private def doneWorker(address: String, pivot: Pivot): Unit = {
        this.synchronized{
            workerDone = workerDone.appended((address, pivot))
            if (workerDone.length == workerCount) {
                // pass workerDone and workers and validation
                // call function in MasterJob.scala
                self.stop()
            }
        }
    }

    private class SorterImpl extends SorterGrpc.Sorter{
        override def registerWorker(req: RegisterRequest) = {
            var workerOrder = -1
            if (workers.length < workerCount) {
                workerOrder = addWorker(req.address)
                workerLatch.countDown()
            }
            workerLatch.await()
            val reply = RegisterResponse(workerOrder = workerOrder)
            Future.successful(reply)
        }

        override def mergeDone(req: DoneRequest) = {
            doneWorker(req.address, req.pivot.get)
            val reply = DoneResponse(ok = true)
            Future.successful(reply)
        }

        
        /** "master-worker" service
          * functionality mainly done by setPivots in MasterJob.scala
          * - wait for all workers request by CountDownLatch 
          */

        /** getPartitionInfo
          * - wait for all workers to send partition info by using CountDownLatch
          * - get all the information from lists and filter each worker Address from other worker's list -> call user defined function from MasterJob
          * - redistribute file names to according worker address -> in form of List[(WorkerAddress, List[FileName])]
          */

        override def getWorkerPivots(responseObserver: StreamObserver[PivotResponse]): StreamObserver[PivotRequest] = {
            new StreamObserver[PivotRequest]() {
                var writer: OutputStream = null
                var status: Status = Status.IN_PROGRESS
                var sampleFileName = null


                override def onNext(req: PivotRequest): Unit = {
                    
                    writer = getFilePath(req)   

                    writeFile(writer, req.file.get.content.toString("UTF-8"))
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