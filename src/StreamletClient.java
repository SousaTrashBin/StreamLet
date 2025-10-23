import utils.ConfigParser;
import utils.application.Transaction;
import utils.communication.Address;
import utils.logs.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class StreamletClient {

    private boolean running = true;
    private Socket socket;
    private BufferedReader userInput;
    private ObjectOutputStream out;

    private Map<Integer, Address> serverAddressees;
    private final Random random = new Random(1L);

    public void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { System.in.close(); } catch (IOException ignored) {}
            closeResources();
        }));

        userInput = new BufferedReader(new InputStreamReader(System.in));
        serverAddressees = ConfigParser.parseServers();
        printInfoGui();
        while (running) {
            connectToRandomStreamlet();
            printClientGui();
            Transaction clientTransaction = handleClientInput(userInput);
            if (clientTransaction != null) {
                sendTransaction(clientTransaction);
                IO.println("Transaction Sent!");
            }
            try {
                Thread.sleep(500);
                socket.close();
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void connectToRandomStreamlet() {
        int randomInt = random.nextInt(serverAddressees.size());
        int port = serverAddressees.get(randomInt).port();
        String ip = serverAddressees.get(randomInt).ip();
        try {
            socket = new Socket(ip, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private Transaction handleClientInput(BufferedReader userInput ) {
        try {
            String input = userInput.readLine();
            if (input.equals("quit")) {
                running = false;
                System.out.println("Closing client...");
                return null;
            }
            Double amount = Double.parseDouble(input.split(" ")[0]);
            int sender = Integer.parseInt(input.split(" ")[1]);
            int receiver = Integer.parseInt(input.split(" ")[2]);
            UUID uuid = UUID.randomUUID();
            long id = uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
            return new Transaction(id, amount, sender, receiver);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void sendTransaction(Transaction transaction) {
        try {
            IO.println("Sending Transaction...");
            out.writeObject(transaction);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void printClientGui() {
        System.out.print("Enter a transaction: ");
    }

    private void printInfoGui() {
        System.out.println("Type transaction as <amount> <sender> <receiver> ");
        System.out.println("To exit type: quit");
        System.out.println("Example: 23.45 2 3");
    }

    private void closeResources() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (userInput != null) {
                userInput.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (out != null) {
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
