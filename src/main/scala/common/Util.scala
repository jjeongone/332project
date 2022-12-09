package cs332.common

import java.net.{DatagramSocket, InetAddress}
import java.nio.file.{Files, Paths}

import java.util.logging.Logger
import java.io.File

object Util {
    private[this] val logger = Logger.getLogger("Util")
    val currentDirectory: String = Paths.get(".").toAbsolutePath.toString.dropRight(1)
    def getIPaddress: String = {
        val socket = new DatagramSocket 
        try {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
            socket.getLocalAddress().getHostAddress()
        } finally if (socket != null) {
            socket.close()
        }
    }
    
    def makeSubdirectory(currentDirectory: String, subDirectory: String): String = {
        val newDir = new File(currentDirectory + subDirectory)
        val newPath = newDir.toString()
        if (newDir.mkdir()) {
            logger.info(newPath + "was created successfully")
        } else {
            logger.info("failed trying to create" + newPath)
        }
        newPath
    }

    def splitEndpoint(endpoint: String): (String, Int) = {
        val address = endpoint.split(":").apply(0)
        val port = endpoint.split(":").apply(1).toInt
        (address, port)
    }

    def readFilesfromDirectory(directory: String): List[File] = {
        var files = List[File]()
        val dir = new File(currentDirectory + directory)

        if (dir.exists && dir.isDirectory) {
            val tmp = dir.listFiles.filter(_.isFile).toList
            files = files ::: tmp
        } else {
            logger.info("Input File directories do not exist or are not directories") 
        }

        files
    }
}