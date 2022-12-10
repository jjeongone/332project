package cs332.worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import java.util.concurrent.CountDownLatch
import cs332.protos.sorting.SorterGrpc._
import cs332.protos.sorting._
import cs332.common.Util.{currentDirectory, getIPaddress, readFilesfromDirectory, splitEndpoint}
import cs332.common.Util
import cs332.worker.WorkerJob._

import java.io.{File, InputStream}
import io.grpc.stub.StreamObserver

import java.nio.file.{Files, Paths}
import com.google.protobuf.ByteString
import worker.client.WorkerFileClient
import worker.server.WorkerFileServer

object Worker {
    def apply(host: String, port: Int): Worker = {
        val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
        val blockingStub = SorterGrpc.blockingStub(channel)
        val newStub = SorterGrpc.stub(channel)
        new Worker(channel, blockingStub, newStub, workerPort)
    }


    def main(args: Array[String]): Unit = {
        val masterEndpoint = args.headOption
        val inputFileDirectory = args.slice(args.indexOf("-I")+1, args.indexOf("-O"))
        val outputFileDirectory = args.slice(args.indexOf("-O")+1, args.indexOf("-N"))
        val tempWorkerOrder = args.slice(args.indexOf("-N")+1, args.length)(0).toInt

        System.out.println("worker order is " + tempWorkerOrder)

        if (masterEndpoint.isEmpty) {
            System.out.println("Master endpoint is not ready")
        }
        else if (inputFileDirectory.isEmpty) {
            System.out.println("Input File Directory is not ready")
        }
        else if (outputFileDirectory.isEmpty) {
            System.out.println("Output File Directory is not ready")
        }
        else {
            val masterIPAddress = splitEndpoint(masterEndpoint.get)._1
            val masterPort = splitEndpoint(masterEndpoint.get)._2
            
            val client = Worker(masterIPAddress, masterPort)
            
            try {
                val succeedRegistration = client.register()
                if (!succeedRegistration){  
                    client.shutdown()

                }
                else {
//                    client.externalSort(inputFileDirectory)
//                    client.sample()
//                    client.sendSample()
//                    client.partitionByPivot()
//                    client.mergeDone()
                    if (tempWorkerOrder == 1) {
                        WorkerFileServer.main(tempWorkerOrder)
                    } else {
                        val tempWorkerList = List(("172.17.0.3:50053", 1), ("172.17.0.4:50053", 2))
                        val shuffle = tempWorkerList.map(worker => {
                            if (worker._2 != tempWorkerOrder) {
                                WorkerFileClient.main(worker._1.split(":")(0), worker._1.split(":")(1).toInt)
                            }
                        })
                        System.out.println("shuffling result: " + shuffle)
                    }
                }
                
            } finally {
                client.shutdown()
            }
        }
    }

    private val workerPort = 50060
}

class Worker private(
    private val channel: ManagedChannel, 
    private val blockingStub: SorterBlockingStub,
    private val newStub: SorterStub,
    private val myPort: Int
) {
    type FileAddress = String
    type WorkerAddress = String

    private val workerDirectory = Util.makeSubdirectory(currentDirectory, "worker") + "/"

    private[this] val logger = Logger.getLogger(classOf[Worker].getName)
    private val myAddress: WorkerAddress = getIPaddress
    private var workerOrder:Int = -1
    private var workerPivots: List[cs332.protos.sorting.Worker]= null 
    
    private val pivotLatch: CountDownLatch = new CountDownLatch(1)
    private var shuffleCandidate: List[String] = null
    private val shufflePath: String = "shuffled"
    private val mergedOutput: String = "mergedOutput"
    val min = "" 
    val max = ""
    def shutdown(): Unit = {
        channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
    }

    def register(): Boolean = {
        
        logger.addHandler(Util.createHandler(workerDirectory, "worker.log"))
        val endpoint = myAddress + ":" + myPort
        // val endpoint = myAddress 
        val request = RegisterRequest(address = endpoint)
        try {
            val response = blockingStub.registerWorker(request)
            var registerSuccess = false
            if (response.workerOrder != -1) {
                workerOrder = response.workerOrder
                logger.info("REGISTER : success as worker " + workerOrder)
                registerSuccess = true
            }
            else {
                logger.info("REGISTER : fail")
            }
            registerSuccess
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
            false
        }
    }

    def getFilesFromDirectories(inputFileDirectory: Array[FileAddress]): List[File] = {
        var inputFiles = List[File]()
        for (address <- inputFileDirectory) {
            val d = new File(currentDirectory + address)
            
            if (d.exists && d.isDirectory) {
                val tmpFiles = d.listFiles.filter(_.isFile).toList
                inputFiles = inputFiles ::: tmpFiles
            } else {
                logger.info("Input File directories do not exist or are not directories") 
            }
        }
        inputFiles
    }
    
    def externalSort(inputFileDirectory: Array[FileAddress]): Unit = {
        val inputFiles = getFilesFromDirectories(inputFileDirectory)
        WorkerJob.externalMergeSort(inputFiles)
        logger.info("EXTERNAL MERGE SORT : done") 
    }

    def sample(): Unit = {
        WorkerJob.sampling()
        logger.info("SAMPLE : sampline is done") 
    }

    def sendSample(): Unit = {
        logger.info("Will try to send file "  + " ...")
        val streamObserver: StreamObserver[PivotRequest] = newStub.getWorkerPivots(
            new StreamObserver[PivotResponse] {
                override def onNext(response: PivotResponse): Unit =  {
                    workerPivots = response.workerPivots.toList
                    pivotLatch.countDown
                    assert(workerPivots != null)
                    logger.info(
                            "File upload status :: " + response.status
                    ) 
                }

                override def onError(throwaable: Throwable): Unit = {}

                override def onCompleted(): Unit = {
                }
            })  
        
        val path = Paths.get(currentDirectory + WorkerJob.sampleFile)


        val metadata = Metadata(fileName = WorkerJob.sampleFile, fileType = workerOrder.toString)

        val inputStream: InputStream = Files.newInputStream(path)
        var bytes = Array.ofDim[Byte](2000000)
        var size: Int = 0
        size = inputStream.read(bytes)
        while (size > 0){
            val content = ByteString.copyFrom(bytes, 0 , size)
            val file = FileMessage(content = content)
            val fileRequest = PivotRequest(metadata = Option(metadata), file = Option(file))
            streamObserver.onNext(fileRequest)
            size = inputStream.read(bytes)
        }
        
        inputStream.close()
        streamObserver.onCompleted()
        logger.info("SAMPLE : sending sample is done") 
        pivotLatch.await()
    }


    /** "worker main" function 
      * functionality: read pivot range from list workerPivots
      * - partition files in sortedOutputFileDirectory by pivot range in workerPivots
      * - save partitioned file in user defined path, partitionedFileDirectory
      * - record list of partition file name in second place of tuple -> save record in var partitionRecord
      * - move my range partitioned file to shuffledFileDirectory
      */
    def partitionByPivot() = {
        val partition = WorkerJob.partitionByPivot(workerPivots, workerOrder)
        logger.info("PARTITION : partitioning is done") 
        val partitionInfo = 
            partition.map{case (address, files) => PartitionInfo(address.toString, files.map(file => file.getName).toSeq)}.toSeq
        val request = PartitionRequest(partitionInfo = partitionInfo)
        try {
            val response = blockingStub.getPartitionInfo(request)
            shuffleCandidate = response.workers.toList
            logger.info("PARTITION : ready to shuffle") 
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
            false
        }   
    }

    /** "master-worker" function 
      * functionality: get information of my partitioned file distribution in other worker
      * - give partitionRecord to master
      * - get List of List[(WorkerAddress, List[PartitionFileName])] from master
      */
    def getPartitionInfo() = ???

    /** "worker-worker" function
      * functionality: shuffle partitioned file based on partitionInfo 
      * - continue shuffling until all partitioned file is received to worker space
      * - if shuffling done start mergeIntoSortedFile
      * - save shuffled file in shuffledFileDirectory
      */
    def shuffle() = ???
 
    /** "worker main" function
      * functionality: merge all received partitioned file in 
      * - call function from WorkerJob.scala
      * - save file in outputFileDirectory 
      * - save min and max value of merged data
      */
    def mergeIntoSortedFile(outputFileDirectory: String) = {
        val shuffleFiles = readFilesfromDirectory(shufflePath)
        WorkerJob.mergeIntoSortedFile(shuffleFiles, currentDirectory + outputFileDirectory + mergedOutput)

    }

    def mergeDone() = {
        val pivot = Option(Pivot(min = min, max = max))
        val request = DoneRequest(address = myAddress, pivot = pivot)
        try {
            val response = blockingStub.mergeDone(request)
            logger.info("TERMINATE")
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        }
    }
} 