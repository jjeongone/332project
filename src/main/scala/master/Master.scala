package cs332.master

import java.util.logging.{Logger, FileHandler, SimpleFormatter}
import io.grpc.{Server, ServerBuilder}

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.io.OutputStream
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Paths, Path}
import java.io.{File, InputStream}
import scala.io.Source
import cs332.protos.sorting._
import cs332.worker.WorkerJob
import cs332.common.Util.{getIPaddress, currentDirectory, makeSubdirectory, splitEndpoint, readFilesfromDirectory}
import java.nio.file.StandardOpenOption
import java.io.IOException
import cs332.common.Util

object Master {
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

    private val port = 50059
}

class Master(executionContext: ExecutionContext, workerCount: Int) {self => 
    private [this] var server: Server = null
    private val masterDirectory = makeSubdirectory(currentDirectory, "master") + "/"
    
    private val logger = Logger.getLogger(classOf[Master].getName)

    private val workerLatch: CountDownLatch = new CountDownLatch(workerCount)
    private var workers: List[Worker] = List()
    private var workerDone: List[(String, Pivot)] = List()
    private var workerPivots: List[Worker] = List()
    private val samplesPath = "sampled"
    private val sortedSamples = "sortedSamples"
    private val SERVER_PATH = Paths.get(makeSubdirectory(currentDirectory, samplesPath))

    private def start(): Unit = {
        logger.addHandler(Util.createHandler(masterDirectory, "master.log"))

        server = ServerBuilder.forPort(Master.port).addService(SorterGrpc.bindService(new SorterImpl, executionContext)).build.start
        logger.info("Server has client Count of " + self.workerCount)
        logger.info("Server started, listening on " + Master.port)
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
            workers = workers.appended(Worker(address, Option(Pivot("", ""))))
            if (workers.length == workerCount) {
                printWorkers()
            }
        }
        workerOrder
    }

    private def printWorkers(): Unit = {
        val workerAddresses = workers.map(worker => splitEndpoint(worker.address)._1)
        System.out.println(workerAddresses.mkString(", "))
    }

    private def doneWorker(address: String, pivot: Pivot): Unit = {
        this.synchronized{
            workerDone = workerDone.appended((address, pivot))
            if (workerDone.length == workerCount) {
                // pass workerDone and workers and validation
                // call function in MasterJob.scala
                
                logger.info("TERMINATE")
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

        override def getWorkerPivots(responseObserver: StreamObserver[PivotResponse]): StreamObserver[PivotRequest] = {
            new StreamObserver[PivotRequest]() {
                var writer: OutputStream = null
                var status: Status = Status.IN_PROGRESS
                var sampleFileName = null


                override def onNext(req: PivotRequest): Unit = {
                    val fileName = req.metadata.get.fileName + "." + req.metadata.get.fileType
         
                    writer = getFilePath(SERVER_PATH, fileName)   

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
                    val sampleFiles = readFilesfromDirectory(samplesPath).filter(file => !(file.toString.contains(sortedSamples)))
                    WorkerJob.mergeIntoSortedFile(sampleFiles, SERVER_PATH + "/" + sortedSamples)
                    val samplesContent = new File(SERVER_PATH + "/" + sortedSamples)
                    workerPivots = MasterJob.setPivot(samplesContent, workers)
                    logger.info("PIVOT : setting pivot is done")
                    val reply = PivotResponse(status = status, workerPivots = workerPivots)

                    responseObserver.onNext(reply)
                    responseObserver.onCompleted()
                }
            }
        }

        override def getPartitionInfo(req: PartitionRequest) = {
            System.out.println(req)
            val reply = PartitionResponse(workers = Seq("1", "2", "3"))
            Future.successful(reply)
        }

        override def workerFileServerManagement(request: FileServerRequest): Future[FileServerResponse] = ???

        private def getFilePath(path: Path, fileName: String): OutputStream = {
            Files.newOutputStream(path.resolve(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
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

        /** getPartitionInfo
          * - wait for all workers to send partition info by using CountDownLatch
          * - get all the information from lists and filter each worker Address from other worker's list -> call user defined function from MasterJob
          * - redistribute file names to according worker address -> in form of List[(WorkerAddress, List[FileName])]
          */

    }
}