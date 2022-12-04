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
}