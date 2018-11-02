import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Router {
  //java Router 127.0.0.1:9999 A 127.0.0.1:10000 B 127.0.0.1:10001 C 127.0.0.1:10002
  static class Operator {
    String key;
    String address;
    int port;
    Operator(String key, String address, int port) {
      this.key = key;
      this.address = address;
      this.port = port;
    }
  }
  public static void main(String[] args) throws IOException{
    String[] bindData = args[0].split(":");
    String bindAddress = bindData[0];
    int bindPort = Integer.parseInt(bindData[1]);

    int operatorSize = 0;
    Operator[] operators = new Operator[(args.length - 1) / 2];
    for (int i = 1; i < args.length; i += 2) {
      String key = args[i];
      String[] operatorData = args[i+1].split(":");
      operators[operatorSize] = new Operator(key, operatorData[0], Integer.parseInt(operatorData[1]));
      operatorSize++;
    }

    ServerSocket listener = new ServerSocket(bindPort);
    try {
      while (true) {
        Socket socket = listener.accept();
        try {
          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
          BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
          //out.println(new Date().toString());
          String inputData = input.readLine();
          String data = inputData.substring(5, inputData.indexOf(" "));
          String[] operatorsToVisit = inputData.substring(inputData.indexOf("OPS:") + 4).split(",");
          System.out.println("data: " + data);
          System.out.print("operatorsToVisit: ");
          for (int i = 0; i < operatorsToVisit.length; i++) {
            System.out.print(operatorsToVisit[i] + " ");
          }
        } finally {
          socket.close();
        }
      }
    }
    finally {
        listener.close();
    }

    /*
    for (int i = 0; i < operators.length; i++) {
      System.out.println(operators[i].key +  " " + operators[i].address + " " + operators[i].port);
    }
    */


  }
}
