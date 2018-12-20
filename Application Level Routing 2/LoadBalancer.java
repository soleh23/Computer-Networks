import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class LoadBalancer {

    static class Operator {
        public String address;
        public int port;
        Operator(String address, int port) {
            this.address = address;
            this.port = port;
        }
    }

    public static void main(String[] args) throws IOException{
        String[] bindData = args[0].split(":");
        String bindAddress = bindData[0];
        int bindPort = Integer.parseInt(bindData[1]);

        //System.out.println(bindAddress + " " + bindPort);
        
        ArrayList<Operator>operators = new ArrayList<Operator>();
        for (int i = 1; i < args.length; i++){
            String[] data = args[i].split(":");
            String address = data[0];
            int port = Integer.parseInt(data[1]);
            operators.add(new Operator(address, port));
        }

        //for (int i = 0; i < operators.size(); i++)
        //    System.out.println(operators.get(i).address + ":" + operators.get(i).port);
    }

}
