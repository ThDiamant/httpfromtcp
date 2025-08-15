import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.exit;

public class Main {

    public static final String NO_MORE_MESSAGES = UUID.randomUUID().toString();

    private static final int BUFFER_SIZE = 8;

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = setUpServerSocket();

            while(true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("A connection has been accepted.");

                DataInputStream clientInputStream = getInputStreamFromSocket(clientSocket);
                BlockingQueue<String> inputQueue = getLinesQueue(clientInputStream);

                printQueueMessages(inputQueue);

                clientSocket.close();
                System.out.println("Connection has been closed.");
            }
        } catch (InterruptedException e) {
            System.out.println("Error while reading from queue: " + e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printQueueMessages(BlockingQueue<String> queue) throws InterruptedException {
        String readValue = queue.take();
        while(!(readValue.equals(NO_MORE_MESSAGES))) {
            System.out.printf("read: %s\n", readValue);
            readValue = queue.take();
        }
    }

    private static ServerSocket setUpServerSocket()
    {
        try {
            ServerSocket serverSocket = new ServerSocket();
            InetSocketAddress socketInetAddress = new InetSocketAddress("127.0.0.1", 42069);
            serverSocket.bind(socketInetAddress);

            return serverSocket;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DataInputStream getInputStreamFromFile(File inputFile) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(inputFile);
        return new DataInputStream(fileInputStream);

    }

    private static DataInputStream getInputStreamFromSocket(Socket socket) throws IOException {
        InputStream clientInputStream = socket.getInputStream();
        return new DataInputStream(clientInputStream);

    }

    private static BlockingQueue<String> getLinesQueue(DataInputStream inputStream)
    {
        BlockingQueue<String> inputQueue = new LinkedBlockingQueue<>();
        Thread.ofVirtual().start(() -> readInputDataIntoQueue(inputStream, inputQueue));

        return inputQueue;
    }

    private static void readInputDataIntoQueue(
            DataInputStream inputStream,
            BlockingQueue <String> inputQueue
    ) {
        byte[] byteSlice;
        String currentLine = "";
        try {
            while ((byteSlice = inputStream.readNBytes(BUFFER_SIZE)).length != 0) {
                String readString = new String(byteSlice, StandardCharsets.UTF_8);
                String[] parts = readString.split("\\r?\\n");

                if (parts.length > 1) {
                    for (int i = 0; i < parts.length - 1; i++) {
                        currentLine = currentLine.concat(parts[i]);
                    }
                    inputQueue.put(currentLine);
                    currentLine = "";
                }

                currentLine = currentLine.concat(parts[parts.length - 1]);
            }

            inputQueue.put(currentLine);
            inputQueue.put(NO_MORE_MESSAGES); // signal message end
            inputStream.close();
        } catch (IOException e) {
            System.out.println("Error while reading from file. Error details:\n" + e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println("Error while putting data on the queue. Error details:\n" + e);
            throw new RuntimeException(e);
        }
    }
}