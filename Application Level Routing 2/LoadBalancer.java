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
        ArrayList<Integer>occs = new ArrayList<Integer>();
        ArrayList<Integer>capacity = new ArrayList<Integer>();
        ArrayList<Integer>resultData = new ArrayList<Integer>();
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

                    if (inputData1[0].equals("DATA")){
                        for (int i = 0; i < formatedInputData1.length; i++)
                            numbers.add(new Integer(Integer.parseInt(formatedInputData1[i])));
                        for (int i = 0; i < formatedInputData2.length; i++)
                            functions.add(new String(formatedInputData2[i]));
                    } else {
                        for (int i = 0; i < formatedInputData2.length; i++)
                            numbers.add(new Integer(Integer.parseInt(formatedInputData2[i])));
                        for (int i = 0; i < formatedInputData1.length; i++)
                            functions.add(new String(formatedInputData1[i]));
                    }

                    //System.out.println("NUMBERS:");
                    //for (int i = 0; i < numbers.size(); i++)
                    //    System.out.print(numbers.get(i) + " ");
                    //System.out.println("\nFUNCTIONS:");
                    //for (int i = 0; i < functions.size(); i++)
                    //    System.out.print(functions.get(i) + " ");

                    Socket[] operatorSocket = new Socket[operators.size()];
                    PrintWriter[] out = new PrintWriter[operators.size()];
                    BufferedReader[] in = new BufferedReader[operators.size()];
                    final String OPERATOR_GET_OCC = "GETOCC:";
                    for (int i = 0; i < operators.size(); i++) {
                        try {
                            operatorSocket[i] = new Socket(operators.get(i).address, operators.get(i).port);

                            out[i] = new PrintWriter(operatorSocket[i].getOutputStream(), true);
                            in[i] = new BufferedReader(new InputStreamReader(operatorSocket[i].getInputStream()));

                            out[i].println(OPERATOR_GET_OCC);
                            String occ = in[i].readLine();
                            occs.add(Integer.parseInt(occ.split(":")[1]));
                        } catch (Exception e) { 
                            //e.printStackTrace();
                            //System.out.println("Reconnecting to operator " + i);
                            i--;
                        } 
                    }

                    int totalCapacity = 0;
                    double pDenominator = 0;
                    for (int i = 0; i < occs.size(); i++)
                        pDenominator += (double)1 / (occs.get(i) + 1);    
                    for (int i = 0; i < occs.size(); i++){
                        double pNumerator = (double)1 / (occs.get(i) + 1);
                        double p = pNumerator / pDenominator;
                        double curCapacity = Math.floor(numbers.size() * p);
                        capacity.add(new Integer((int)curCapacity));
                        totalCapacity += (int)curCapacity;
                    }
                    
                    capacity.set(0, numbers.size() - totalCapacity + capacity.get(0));
                    
                    //for (int i = 0; i < capacity.size(); i++)
                        //System.out.println(capacity.get(i));
                    
                    for (int i = 0; i < numbers.size(); i++)
                        resultData.add(numbers.get(i));

                    final String OPERATOR_END = "END:";
                    for (int i = 0; i < functions.size(); i++){
                        String toSendFunc = "FUNCS:" + functions.get(i);
                        int startIndex = 0;
                        for (int j = 0; j < operators.size(); j++){
                            try {
                                if (capacity.get(j) > 0){
                                    String toSendData = "DATA:";
                                    for (int k = startIndex; k < startIndex + capacity.get(j).intValue() - 1; k++)
                                        toSendData += resultData.get(k) + ",";
                                    toSendData += resultData.get(startIndex + capacity.get(j).intValue() - 1);

                                    out[j].println(toSendData);
                                    out[j].println(toSendFunc);

                                    startIndex += capacity.get(j).intValue();
                                }
                            } catch (Exception e) { 
                                e.printStackTrace();
                            } 
                        }
                        startIndex = 0;
                        for (int j = 0; j < operators.size(); j++){
                            try {
                                if (capacity.get(j) > 0){
                                    String[] response = in[j].readLine().split(":")[1].split(",");
                                    
                                    if (i == functions.size() - 1)
                                        out[j].println(OPERATOR_END);
                                    
                                    for (int k = startIndex; k < startIndex + capacity.get(j).intValue(); k++){
                                        resultData.set(k, new Integer(Integer.parseInt(response[k - startIndex])));
                                    }

                                    startIndex += capacity.get(j).intValue();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                if (operatorSocket[j] != null && i == functions.size() - 1)
                                    operatorSocket[j].close();
                            }
                        }
                    }

                    String resultDataToSend = "DATA:";
                    for (int i = 0; i < resultData.size() - 1; i++)
                        resultDataToSend += resultData.get(i) + ",";
                    resultDataToSend += resultData.get(resultData.size() - 1); 
                    
                    PrintWriter resultOut = new PrintWriter(socket.getOutputStream(), true);
                    resultOut.println(resultDataToSend);
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
