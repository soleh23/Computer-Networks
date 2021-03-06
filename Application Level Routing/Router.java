import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Router {

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

  public static String getAddress(String key) {
    for (int i = 0; i < operators.length; i++)
      if (operators[i].key.equals(key))
        return operators[i].address;
    return null;
  }

  public static int getPort(String key) {
    for (int i = 0; i < operators.length; i++)
      if (operators[i].key.equals(key))
        return operators[i].port;
    return -1;
  }

  static Operator[] operators;

  public static void main(String[] args) throws IOException{
    String[] bindData = args[0].split(":");
    String bindAddress = bindData[0];

    int bindPort = Integer.parseInt(bindData[1]);
    int operatorSize = 0;

    operators = new Operator[(args.length - 1) / 2];

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
          BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

          String inputData = input.readLine();
          String data = inputData.substring(0, inputData.indexOf(" "));
          String[] operatorsToVisit = inputData.substring(inputData.indexOf("OPS:") + 4).split(",");

          Socket operatorSocket = null;
          for (int i = 0; i < operatorsToVisit.length; i++) {
            try {
              operatorSocket = new Socket(getAddress(operatorsToVisit[i]), getPort(operatorsToVisit[i]));

              PrintWriter out = new PrintWriter(operatorSocket.getOutputStream(), true);
              BufferedReader in = new BufferedReader(new InputStreamReader(operatorSocket.getInputStream()));

              out.println(data);
              data = in.readLine();

            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              operatorSocket.close();
            }
          }

          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
          out.println(data);
        } finally {
          socket.close();
        }
      }
    }
    finally {
      listener.close();
    }
  }
}
