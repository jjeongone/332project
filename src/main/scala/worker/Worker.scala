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
        new Worker(channel, blockingStub)
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
                    client.externalSort(inputFileDirectory)
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
    private val blockingStub: SorterBlockingStub
) {
    private[this] val logger = Logger.getLogger(classOf[Worker].getName)
    private val address: String = getIPaddress
    private var min: String = "0"
    private var max: String = "1"

    def shutdown(): Unit = {
        channel.shutdown.awaitTermination(600, TimeUnit.SECONDS)
    }

    def register(): Boolean = {
        val request = RegisterRequest(address = address)
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

    /*
    Given list of address, external sort function uses reads file from such addresss and sorts file into some other address. 
    - functionality: files in directorys are made into one file in temporary address
    Heewoo:
        V pass Input address and check if address is successfully passed 
        - do not return any return value 
    */
    def externalSort(inputFileDirectory: Array[String]): Unit = {
        inputFileDirectory.foreach(System.out.println)
    }

    /*
    sample function extracts some data from sorted file and gives output of file 
    such file need to be passed to server

    Heewoo: 
        - address of external sorted file is given and as a output sample file is given
    
    PseudoWorkerJob: 
        - returns file with some values in it 
    */
    def sample(): File = ???

    def getPivots() = {
        val pivot = Option(Pivot(min = min, max = max))
        val request = DoneRequest(address = address, pivot = pivot)
        try {
            val response = blockingStub.mergeDone(request)
            logger.info("alerted master that merging is done and terminating...")
        } 
        catch {
            case e: StatusRuntimeException =>
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        }
    }

    /*
    extracts pivot range from distributed worker-pivot list and partition file 

    PseudoWorkerJob: 
        - returns partitioned List[(Worker, List(Partitioned file names))]
        
    Message Relation: 
        - gets partition file count and send to master
        - 
    */
    def partition() = ???

    /*
    shuffle between workers 
    */
    def shuffle() = ???
 
    /*
    merge happens in between after shuffle 
    */
    def merge() = ???

    def mergeDone() = {
        val pivot = Option(Pivot(min = min, max = max))
        val request = DoneRequest(address = address, pivot = pivot)
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