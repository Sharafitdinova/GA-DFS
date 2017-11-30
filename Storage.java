import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;
import java.net.ServerSocket;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;


public class Storage {
    static Scanner scanner = new Scanner(System.in);
    static int port;
    static NamingServerThread clientThread;

    public static void main(String[] args) {
        ServerSocket servSock;
        String ip;


        System.out.print("Name Server IP: ");
        String namingServerHost = scanner.nextLine();
        System.out.print("Name Server Port: ");
        int namingServerPort = Integer.parseInt(scanner.nextLine());

        System.out.print("Local Port: ");
        port = Integer.parseInt(scanner.nextLine());


        try {

            servSock = new ServerSocket(port);
            ip = servSock.getInetAddress().getHostAddress() + ":" + servSock.getLocalPort();
            clientThread = new NamingServerThread(port, namingServerHost, namingServerPort);
        } catch (IOException e) {
            System.err.println("Can't start storage and register server.\n" + e.getMessage());
            return;
        }

        if (createRootDirectory(port)) {
            while (true) {
                try {
                    Socket newConnection = servSock.accept();
                    StorageServerUser user = new StorageServerUser(newConnection);
                    user.start();
                } catch (IOException e) {
                    System.err.println("Error establishing connection. Reason: " + e.getMessage());
                }
            }
        } else {
            System.err.println("Storage server shutting down because it could not create its root directory!");
        }
    }

    private static boolean createRootDirectory(int port) {
        File portDir = new File(String.valueOf(port));
        if (!portDir.exists()) {
            if (portDir.mkdir()) {
                System.out.println("Port directory was created!");
                File root = new File(port + "/" + "home");
                if (!root.exists()) {
                    if (root.mkdir()) {
                        System.out.println("Root directory was created!");
                        return true;
                    } else {
                        System.err.println("Failed to create root directory!");
                    }
                }
            } else {
                System.err.println("Failed to create port directory!");
            }
        } else {
            return true;
        }

        return false;
    }
}


class StorageServerUser extends Thread {
    Socket mySocket;
    DataInputStream in;
    DataOutputStream out;

    public StorageServerUser(Socket s) throws IOException {
        this.mySocket = s;
        this.in = new DataInputStream(s.getInputStream());
        this.out = new DataOutputStream(s.getOutputStream());
    }

    public void run() {
        try {
            this.startUserThread();
        } catch (IOException e) {
            System.out.println("Client Disconnected");
        } finally {
            closeConnection();
        }
    }

    private void startUserThread() throws IOException {
        String message;
        do {
            message = this.in.readUTF().toLowerCase();
            if (message.startsWith("info_from_storage")) {
                onInfoCommand(message);
            } else if (message.startsWith("read_from_storage_server")) {
                onReadCommand(message);
            } else if (message.startsWith("write_from_storage_server")) {
                onWriteCommand(message);
            }
        } while (true);
    }

    private void onInfoCommand(String message) {
        String filePath = message.split("~!~")[1];
        String localFilePath = Storage.port + "/" + filePath;
        StringBuilder response =  new StringBuilder();
        File file = new File(localFilePath);
        if (file.exists()) {
            response.append("success");
            response.append("~!~");
            response.append("Path: " + filePath);
            response.append(System.getProperty("line.separator"));
            response.append("Size: " + file.length() + " bytes");
            response.append(System.getProperty("line.separator"));
            String lastModified = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
                    .format(new Date(file.lastModified()));
            response.append("Last modified: " + lastModified);
            response.append(System.getProperty("line.separator"));
            response.append("Node ID: " + Storage.port);
        } else {
            response.append("error");
            response.append("~!~");
            response.append("File '" + filePath + "' does not exist in this storage server.");
        }

        send(response.toString());
    }

    private void onReadCommand(String message) {
        String filePath = message.split("~!~")[1];
        String localFilePath = Storage.port + "/" + filePath;
        StringBuilder response =  new StringBuilder();
        File file = new File(localFilePath);
        if (file.exists()) {
            try(BufferedReader br = new BufferedReader(new FileReader(file))){
                response.append("success");
                response.append("~!~");
                String line = "";
                while((line = br.readLine()) != null){
                    response.append(line);
                    response.append(System.getProperty("line.separator"));
                }
            } catch(IOException e){
                response.append("error");
                response.append("~!~");
                response.append(e.getMessage());
            }
        } else {
            response.append("error");
            response.append("~!~");
            response.append("File '" + filePath + "' does not exist in this storage server.");
        }

        send(response.toString());
    }

    private void onWriteCommand(String message) {
        String[] data = message.split("~!~");
        String filePath = data[1];
        String localFilePath = Storage.port + "/" + filePath;
        String contents = data[2];
        StringBuilder response =  new StringBuilder();
        File file = new File(localFilePath);
        if (!file.exists()) {
            try {
                file.getParentFile().mkdirs();
                if (file.createNewFile()) {
                    Files.write(Paths.get(localFilePath), contents.getBytes());
                    response.append("success");
                    response.append("~!~");
                    response.append("File '" + filePath + "' successfully created.");
                } else {
                    response.append("error");
                    response.append("~!~");
                    response.append("Failed to create file '" + filePath + "'");
                }
            } catch(IOException e){
                response.append("error");
                response.append("~!~");
                response.append(e.getMessage());
            }
        } else {
            response.append("error");
            response.append("~!~");
            response.append("File '" + filePath + "' already exists in this storage server.");
        }

        send(response.toString());
    }

    void send(String message) {
        try {
            this.out.writeUTF(message);
            this.out.flush();
        } catch (IOException e) {
            System.err.println("CANT'T SEND MESSAGE");
            this.closeConnection();
        }
    }

    public void closeConnection() {
        try {
            if (this.in != null)
                in.close();
            if (this.out != null)
                out.close();
            if (this.mySocket != null)
                mySocket.close();
        } catch (IOException e) {
            System.err.println("ERR: Can't close user socket. Reason: " + e.getMessage());
        }
    }
}


class NamingServerThread extends Thread {
    private Socket namingServerSocket;
    private DataInputStream namingServerIn;
    private DataOutputStream namingServerOut;

    public NamingServerThread(int storageServerPort, String namingServerHost, int namingServerPort) throws IOException {
        namingServerSocket = new Socket(namingServerHost, namingServerPort);
        namingServerIn = new DataInputStream(namingServerSocket.getInputStream());
        namingServerOut = new DataOutputStream(namingServerSocket.getOutputStream());
        namingServerOut.writeUTF("register" + "~!~" + storageServerPort);
        namingServerOut.flush();
        String response = namingServerIn.readUTF();
        if (!response.equals("success")) {
            throw new IOException("Could not register with the Naming server");
        }

        System.out.println("CONNECTED TO NAMING SERVER");
        this.start();
    }

    public void run() {
        try {
            while (true) {
                String message = this.namingServerIn.readUTF();
                String[] split = message.split("~!~");
                if (split.length >= 1) {
                    String type = split[0];
                    switch (type) {
                        case "delete_from_storage":
                            onDeleteCommand(message);
                            break;
                        case "size":
                            onSize();
                            break;
                        case "replica_to_storage":
                            onReplica(split[1], split[2]);
                            break;
                        default:
                            break;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("CONNECTION LOST");
            this.close();
        }
    }

    private void onSize(){
        String filePath = Storage.port + "/" + "home";
        File file = new File(filePath);
        System.out.println("MEMORY: " + file.getUsableSpace());
        send(String.valueOf(file.getUsableSpace()));
    }

    private void onReplica(String filePath, String address){
        String localFilePath = Storage.port + "/" + filePath;
        String[] splitAddress = address.split(":");
        String ip = splitAddress[0];
        int port = Integer.parseInt(splitAddress[1]);
        try (Socket storageServer = new Socket(ip, port);
             DataInputStream ssIn = new DataInputStream(storageServer.getInputStream());
             DataOutputStream ssOut = new DataOutputStream(storageServer.getOutputStream());
             BufferedReader reader = new BufferedReader(new FileReader(localFilePath))) {
            StringBuilder contents = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                contents.append(line);
                contents.append(System.getProperty("line.separator"));
            }

            String message = "write_from_storage_server" + "~!~" + filePath + "~!~"
                    + contents.toString();
            ssOut.writeUTF(message);
            ssOut.flush();
            String response = ssIn.readUTF();
            String[] splitResponse = response.split("~!~");
            String result = splitResponse[0];
            String resData = splitResponse[1];
            if (result.equals("success")) {
                System.out.println("Replica on " + address + ": " + resData);
                String namingServerMessage = "success_replica_write_to_storage_server" + "~!~" + address
                        + "~!~" + filePath;
                send(namingServerMessage);
            } else {
                onReplicaCreationFail();
                System.out.println("Sorry, something went wrong and your replica was not created.\n" + resData);
            }
        } catch (IOException e) {
            onReplicaCreationFail();
            System.err.println("Sorry, replica not created because the specified file could not be read, try again later!\n" + e.getMessage());
            System.err.println(e.getMessage());
        }
    }

    private void onReplicaCreationFail(){
        send("error");
    }

    private void onDeleteCommand(String message) {
        String filePath = Storage.port + "/" + message.split("~!~")[1];
        StringBuilder response = new StringBuilder();
        File file = new File(filePath);
        File directory = file.getParentFile();
        if (file.exists()) {
            if (file.delete()) {
                response.append("success");
                response.append("~!~");
                deleteEmptyDirectories(directory);
            } else {
                response.append("error");
                response.append("~!~");
                response.append("File '" + filePath + "' could not be deleted from this storage server.");
            }
        } else {
            response.append("error");
            response.append("~!~");
            response.append("File '" + filePath + "' does not exist in this storage server.");
        }

        send(response.toString());
    }

    private void deleteEmptyDirectories(File directory){
        String name = directory.getName();
        if(name.equals("home")){
            return;
        }

        File parent = directory.getParentFile();
        File[] children = directory.listFiles();
        if(children.length <= 0){
            directory.delete();
            deleteEmptyDirectories(parent);
        }
    }

    void send(String message) {
        try {
            this.namingServerOut.writeUTF(message);
            this.namingServerOut.flush();
        } catch (IOException e) {
            System.err.println("CANT'T SEND MESSAGE");
            this.close();
        }
    }



    void close() {
        try {
            if (this.namingServerIn != null)
                namingServerIn.close();
            if (this.namingServerOut != null)
                namingServerOut.close();
            if (this.namingServerSocket != null)
                namingServerSocket.close();
        } catch (IOException e) {
        } finally {
            this.interrupt();
        }
    }
}

