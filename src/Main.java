import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.exit;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static final String NO_MORE_MESSAGES = UUID.randomUUID().toString();

    private static final int BUFFER_SIZE = 8;

    public static void main(String[] args) {
        File inputFile = new File("data/messages.txt");

        try {
            DataInputStream inputStream = getInputStreamFromFile(inputFile);
            BlockingQueue<String> inputQueue = getLinesQueue(inputStream);

            while(true) {
                String readValue = inputQueue.take();
                if (readValue.equals(NO_MORE_MESSAGES)) {
                    break;
                }
                System.out.printf("read: %s\n", readValue);
            }

            exit(0);
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println("Error while reading from queue: " + e);
            throw new RuntimeException(e);
        }
    }

    private static DataInputStream getInputStreamFromFile(File inputFile) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(inputFile);
        return new DataInputStream(fileInputStream);

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