import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStreamReader;

public class Main {
  public static void main(String[] args){
        System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
            InputStream inputStream = clientSocket.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            }
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          clientSocket = serverSocket.accept();
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
