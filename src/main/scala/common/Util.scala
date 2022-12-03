package cs332.common

import java.net.{DatagramSocket, InetAddress}
import java.nio.file.{Files, Paths}

object Util {
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
}