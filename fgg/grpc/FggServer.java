package fgg.grpc;

import io.grpc.*;
import fgg.utils.*;

public class FggServer
{
    public static void main( String[] args ) throws Exception
    {
      Cache2.init();

      // Create a new server to listen on port 8080
      Server server = ServerBuilder.forPort(33789)
        .addService(new FggService2())
        .build();

      // Start the server
      server.start();

      // Server threads are running in the background.
      System.out.println("Server started");

      // Don't exit the main thread. Wait until server is terminated.
      server.awaitTermination();
    }
}
