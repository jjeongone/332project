// C:\Users\owner\Documents\heewoo\332project\target\scala-2.13\src_managed\main\scalapb\com\example\protos\registration\RegisterReply.scala

package com.example.protos.registration

import java.util.logging.logger

import com.example.protos.registration.{RegistratorGrpc, RegisterRequest, RegisterReply}

import scala.concurrent.{ExecutionContext}
import scala.concurrent.Future

object RegistrationServer {
    private val logger = Logger.getLogger(classOf[RegistrationServer].getName)

    def main(args: Array[String]): Unit = {
        val server = new RegistrationServer(ExecutionContext.global)
        server.start()
        server.blockUntilShutDown()
    }

    private val port = 50051
}

class RegistrationServer(executionContext: ExecutionContext) { self =>
    private[this] var server: Server = null

    private def start(): Unit = {
        server = ServerBuilder.forPort(RegistrationServer.port).addService(RegistratorGrpc.bindService(new RegistratorImpl, executionContext)).build.start
        RegistrationServer.logger.info("Server started, listening on" + RegistrationServer.port)
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

    private def blockUntilShutDown(): Unit = {
        if (server != null) {
            server.shutdown()
        }
    }

    private class RegistratorImpl extends RegistratorGrpc.Registrator {
        override def register(request: Any): Future[RegisterReply] = {
            val reply = RegisterReply(message = "Hello " + req.name)
            Future.successful(reply)
        }
    }
}