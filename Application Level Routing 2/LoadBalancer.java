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

        ArrayList<Integer>numbers = new ArrayList<Integer>();
        ArrayList<String>functions = new ArrayList<String>();
        ServerSocket listener = new ServerSocket(bindPort);
        try {
            while (true) {
                Socket socket = listener.accept();
                try {
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    
                    String[] inputData1 = input.readLine().split(":");
                    String[] inputData2 = input.readLine().split(":");

                    String[] formatedInputData1 = inputData1[1].split(",");
                    String[] formatedInputData2 = inputData2[1].split(",");

                    if (inputData1.equals("DATA")){
                        for (int i = 1; i < formatedInputData1.length; i++)
                            numbers.add(new Integer(Integer.parseInt(formatedInputData1[i])));
                        for (int i = 1; i < formatedInputData2.length; i++)
                            functions.add(new String(formatedInputData2[i]));
                    } else {
                        for (int i = 1; i < formatedInputData2.length; i++)
                            numbers.add(new Integer(Integer.parseInt(formatedInputData2[i])));
                        for (int i = 1; i < formatedInputData1.length; i++)
                            functions.add(new String(formatedInputData1[i]));
                    }

                    //System.out.println("NUMBERS:");
                    //for (int i = 0; i < numbers.size(); i++)
                    //    System.out.print(numbers.get(i) + " ");
                    //System.out.println("\nFUNCTIONS:");
                    //for (int i = 0; i < functions.size(); i++)
                    //    System.out.print(functions.get(i) + " ");

                    Socket operatorSocket = null;
                    final String OPERATOR_GET_OCC = "GETOCC:\n";
                    for (int i = 0; i < operators.size(); i++) {
                        try {
                            operatorSocket = new Socket(operators.get(i).address, operators.get(i).port);

                            PrintWriter out = new PrintWriter(operatorSocket.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(operatorSocket.getInputStream()));

                            out.println(OPERATOR_GET_OCC);
                            String occ = in.readLine();
                            System.out.println(i + " --> " + occ);

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            operatorSocket.close();
                        }
                    }

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
