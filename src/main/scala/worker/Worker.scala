package cs332.worker

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import cs332.protos.sorting.SorterGrpc.SorterBlockingStub
import cs332.protos.sorting._
import cs332.common.Util.getIPaddress
import java.io.File

object Worker {
    def apply(host: String, port: Int): Worker = {
        val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
        val blockingStub = SorterGrpc.blockingStub(channel)
        val newStub = SorterGrpc.newStub(channel)
        new Worker(channel, blockingStub, newStub)
    }


    def main(args: Array[String]): Unit = {
        val masterEndpoint = args.headOption
        val inputFileDirectory = args.slice(args.indexOf("-I")+1, args.indexOf("-O"))
        val outputFileDirectory = args.slice(args.indexOf("-O")+1, args.length)

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
            val masterIPAddress = masterEndpoint.get.split(":").apply(0)
            val masterPort = masterEndpoint.get.split(":").apply(1).toInt
            
            val client = Worker(masterIPAddress, masterPort)
            
            try {
                val succeedRegistration = client.register()
                if (!succeedRegistration){  
                    client.shutdown()

                }
                else {
                    client.externalMergeSort(inputFileDirectory)
                    //client.sample()
                    
                    client.mergeDone()
                }
                
            } finally {
                client.shutdown()
            }
        }
    }
}

class Worker private(
    private val channel: ManagedChannel, 
    private val blockingStub: SorterBlockingStub,
    private val newStub: SorterStub
) {
    type Pivot = (String, String)
    type WorkerAddress = String 
    type FileAddress = String
    type Worker = (WorkerAddress, Pivot)

    private[this] val logger = Logger.getLogger(classOf[Worker].getName)
    private val myAddress: WorkerAddress = getIPaddress
    private var min: String = "0"
    private var max: String = "1"
    private var sortedFileDirectory: FileAddress = null
    private var partitionedFileDirectory: FileAddress = null
    private var shuffledFileDirectory: FileAddress = null
    private var samples: List[String] = null 
    private var workerPivots: List[Worker] = null 
    private var partitionRecord: List[(WorkerAddress, List[String])] = null 
    private var partitionInfo: List[(WorkerAddress, List[String])] = null 

    def shutdown(): Unit = {
        channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
    }

    def register(): Boolean = {
        val request = RegisterRequest(address = myAddress)
        try {
            val response = blockingStub.registerWorker(request)
            if (response.success) {
                logger.info("Registration Success")
            }
            else {
                logger.info("Registration Failed")
            }
            response.success
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
            false
        }
    }

    /** "worker main" function 
      * Given list of address, external sort function uses adddresses to read file and sort those files.
      * Sorted temporary files would be placed in user defined path or in same path with different name.
      * - call user function "externalMergeSort" in WorkerJob.scala
      * - save output file path in sortedFileDirectory
      */
    def externalMergeSort(inputFileDirectory: Array[FileAddress]): Unit = ???

    /** "worker main" function 
      * functionality: sample function extracts some data from sorted file and gives output as File format
      *  - extracts samples from sorted file stored
      *  - saves sample in Array of samples 
      * - call user sampling function in WorkerJob.scala
      */
    def sampling(): Unit = {
        StreamObserver[PivotRequest] streamObserver = newStub.getWorkerPivots(new FileUploadObserver())
        
        Path path = Paths.get()

        Metadata metadata = Metadata(address = myAddress, name = "sample", fileType = ".1")
        val metaRequest = PivotRequest(metadata = metadata)

        streamObserver.onNext(metadata)

        InputStream inputStream = Files.newInputStream(path)
        var bytes = Array.ofDim[Byte](4096)
        var size: Int = 0
        while (size = inputStream.read(bytes) > 0){
            val file = File()
            val fileRequest = PivotRequest()
            streamObserver.onNext(request)
        }
    }

    /* "master-worker" function
     * functionality: send request to master with samples and get pivot and address information of all workers
     * - send request with samples array and worker address
     * - get list of pivot range and address of other workers -> saved in workerPivots
     */
    def getWorkerPivots() = ???

    /** "worker main" function 
      * functionality: read pivot range from list workerPivots
      * - partition files in sortedOutputFileDirectory by pivot range in workerPivots
      * - save partitioned file in user defined path, partitionedFileDirectory
      * - record list of partition file name in second place of tuple -> save record in var partitionRecord
      * - move my range partitioned file to shuffledFileDirectory
      */
    def partitionByPivot() = ???

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
    def shuffling() = ???
 
    /** "worker main" function
      * functionality: merge all received partitioned file in 
      * - call function from WorkerJob.scala
      * - save file in outputFileDirectory 
      * - save min and max value of merged data
      */
    def mergeIntoSortedFile() = ???

    def mergeDone() = {
        val pivot = Option(Pivot(min = min, max = max))
        val request = DoneRequest(address = myAddress, pivot = pivot)
        try {
            val response = blockingStub.mergeDone(request)
            logger.info("alerted master that merging is done and terminating...")
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        }
    }
} 