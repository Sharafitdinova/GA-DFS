import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
//import org.omg.CORBA.RepositoryIdHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;




public class NameServer {
    static Scanner scanner = new Scanner(System.in);
    private static ConcurrentHashMap<String, NamingServerUser> storageServers = new ConcurrentHashMap<String, NamingServerUser>();
    public static ReplicaMaker replicaMaker;

    public static void main(String[] args) {

        ServerSocket servSock;

        System.out.print("Name Server Port: ");

        int port = Integer.parseInt(scanner.nextLine());

        try {

            servSock = new ServerSocket(port);
            replicaMaker = new ReplicaMaker();
            replicaMaker.start();
        } catch (IOException e) {
            System.err.println("Can't start naming server");
            return;
        }
        System.out.println("Naming server started");
        File root = new File("home");
        if (!root.exists()) {
            if (root.mkdir()) {
                System.out.println("Root directory was created!");
            } else {
                System.err.println("Failed to create root directory!");
            }
        }

        while (true) {
            try {
                Socket newConnection = servSock.accept();
                NamingServerUser user = new NamingServerUser(newConnection);
                user.start();
            } catch (IOException e) {
                System.err.println("Error establishing connection. Reason: " + e.getMessage());
            }
        }
    }

    public static void addStorageServer(String storageServerAddress, NamingServerUser user){
        storageServers.put(storageServerAddress, user);
    }

    public static NamingServerUser getStorageServerByAddress(String storageServerAddress){
        return storageServers.get(storageServerAddress);
    }

    public static boolean isAddressValid(String address){
        return storageServers.get(address) != null;
    }

    public static synchronized long getAvailableStorageSize(){
        long size = 0;
        for(NamingServerUser storage : storageServers.values()){
            storage.send("size");
            try {
                String currentSize = storage.in.readUTF();
                size += Long.parseLong(currentSize);
            } catch (IOException e) {
                System.err.println("Could not get the size of a storage server.");
                e.printStackTrace();
            }
        }

        return size;
    }

    public static String removeStorageServer(NamingServerUser user){
        String key = null;
        for(String k : storageServers.keySet()){
            if(user.equals(storageServers.get(k))){
                key = k;
                break;
            }
        }

        if(key != null){
            storageServers.remove(key);
            return key;
        }

        return null;
    }

    public static String getAvailableStorageServerForWriting(){
        return getAvailableStorageServerForWriting("");
    }

    public static String getAvailableStorageServerForWriting(String except){
        Enumeration<String> addresses = storageServers.keys();
        int rand = new Random().nextInt(storageServers.size());
        while(addresses.hasMoreElements()){
            String address = addresses.nextElement();
            rand--;
            if(rand <= 0 && !address.equals(except)){
                return address;
            }
        }

        return null;
    }

}






class NamingServerUser extends Thread {
    String currentDir;
    Socket mySocket;
    DataInputStream in;
    DataOutputStream out;
    boolean isStorageServerConnection = false;
    boolean isClosed = false;

    public NamingServerUser(Socket s) throws IOException {
        this.mySocket = s;
        this.in = new DataInputStream(s.getInputStream());
        this.out = new DataOutputStream(s.getOutputStream());
        this.currentDir = "home";
    }

    public void run() {
        try {
            this.startUserThread();
        } catch (IOException e) {
            System.err.println("Client Disconnected");
        } finally {
            closeConnection();
        }
    }

    private void startUserThread() throws IOException {
        String message;
        do {
            message = this.in.readUTF().toLowerCase();
            if (message.equals("reset")) {
                onInitCommand();
            } else if (message.startsWith("cat")) {
                onReadCommand(message, "read_from_storage_server");
            } else if (message.startsWith("nano")) {
                onWriteCommand(message);
            } else if (message.startsWith("rm")) {
                onDeleteCommand(message);
            } else if (message.startsWith("info")) {
                onReadCommand(message, "info_from_storage");
            } else if (message.startsWith("cd")) {
                onOpenCommand(message);
            } else if (message.equals("ls")) {
                onListCommand(message);
            } else if (message.startsWith("mkdir")) {
                onMakeCommand(message);
            } else if (message.startsWith("rmdir")) {
                onRemoveCommand(message);
            } else if (message.equals("exit")) {
                break;
            } else if (message.startsWith("register")) {
                registerStorageServer(message);
            } else if (message.startsWith("success_write_to_storage_server")) {
                successInWritingToStorageServer(message);
            } else {
                send("message" + "~!~" + "Command Not Found");
            }
        } while (!isStorageServerConnection);

        int tries = 0;
        do {
            try {
                Thread.sleep(1000);
                this.out.writeUTF("ping" + "~!~" + "ping");
                tries = 0;
            } catch (InterruptedException e) {
                tries++;
                System.err.println("Exception during ping.");
                e.printStackTrace();
            } catch (IOException ex) {
                tries++;
            }

            if(tries > 3){
                onNotRespondingStorageServer();
                closeConnection();
            }
        } while (!isClosed);
    }

    private void onNotRespondingStorageServer() {
        // move files from this server
        // remove the server from the list
        String address = NameServer.removeStorageServer(this);
        File file = new File("home");
        createReplicas(file, address);
        System.out.println("-------------------Ping to " + address + " not successful!");
    }

    private void createReplicas(File file, String failedAddress){
        File[] children = file.listFiles();
        for(int i = 0; i < children.length; i++){
            File child = children[i];
            if(child.isDirectory()){
                createReplicas(child, failedAddress);
            } else {
                try {
                    List<String> addresses = Files.readAllLines(Paths.get(child.getAbsolutePath()));
                    if(addresses.contains(failedAddress)){
                        addresses.remove(failedAddress);
                        if(addresses.size() > 0){
                            String currAddress = addresses.get(0);
                            Files.write(Paths.get(child.getAbsolutePath()), currAddress.getBytes());
                            String filePath = child.getAbsolutePath()
                                    .substring(child.getAbsolutePath().indexOf("home"));
                            NameServer.replicaMaker.addToQueue(filePath, currAddress);
                        }else{
                            System.out.println("Can't create a replica of " + child.getAbsolutePath() +
                                    " as it is present only on a failed server.");
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Can't create replicas on storage server fail because can't read file " + child.getAbsolutePath());
                }

            }
        }
    }

    private void onReadCommand(String message, String type) throws IOException {
        String fileName = message.split("~!~")[1];
        String response = "message" + "~!~";
        String path = this.currentDir + "/" + fileName;
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                response += "'" + fileName + "' is a directory. You can open it but not read it like a file.";
            } else {
                List<String> content = Files.readAllLines(Paths.get(path));
                String address = content.get(0).trim();
                if (!NameServer.isAddressValid(address)) {
                    if (content.size() > 1) {
                        address = content.get(1).trim();
                    }
                }

                response = type + "~!~" + address + " " + path;
            }
        } else {
            String dirName = "_" + fileName.replaceAll(".txt", "");
            String dirPath = this.currentDir + "/" + dirName;
            File dir = new File(dirPath);
            if (dir.exists()) {
                String[] files = dir.list();
                Arrays.sort(files);
                response = type;
                for (String f : files) {
                    String currName = this.currentDir + "/" + dirName + "/" + f;
                    List<String> content = Files.readAllLines(Paths.get(currName));
                    String address = content.get(0).trim();
                    if (!NameServer.isAddressValid(address)) {
                        if (content.size() > 1) {
                            address = content.get(1).trim();
                        }
                    }

                    response += "~!~" + address + " " + currName;
                }
            } else {
                response += "Can't read file '" + fileName + "' because it does not exist in the current directory.";
            }
        }

        send(response);
    }

    private void onWriteCommand(String message) throws IOException {
        String fileName = message.split("~!~")[1];
        String response = "message" + "~!~";
        String path = this.currentDir + "/" + fileName;
        File file = new File(path);
        if (!file.exists()) {
            String address = NameServer.getAvailableStorageServerForWriting();
            if (address != null) {
                response = "write_from_storage_server" + "~!~" + address + " " + path;
            } else {
                response += "There are no available storage servers to which you can write your file. Sorry.";
            }
        } else {
            response += "File with name '" + fileName + "' already exists in the current directory.";
        }

        send(response);
    }

    private void onDeleteCommand(String message) {
        String fileName = message.split("~!~")[1];
        String response = "message" + "~!~";
        String path = this.currentDir + "/" + fileName;
        File file = new File(path);
        response += deleteFile(file);
        send(response);
    }

    private synchronized String deleteFile(File file) {
        String response = "";
        String path = file.getPath();
        String fileName = file.getName();
        if (file.exists()) {
            if (!file.isDirectory()) {
                try {
                    List<String> content = Files.readAllLines(Paths.get(path));
                    for (int a = 0; a < content.size(); a++) {
                        String address = content.get(a).trim();
                        NamingServerUser storage = NameServer.getStorageServerByAddress(address);
                        if (storage != null) {
                            storage.out.writeUTF("delete_from_storage" + "~!~" + path);
                            String storageResponse = storage.in.readUTF();
                            String[] splitStorageResponse = storageResponse.split("~!~");
                            String result = splitStorageResponse[0];
                            if (result.equals("success")) {
                                file.delete();
                                response += "'" + fileName + "' was successfully deleted\n";

                            } else {
                                String error = splitStorageResponse[1];
                                response += "'" + fileName + "' not successfully deleted.\n" + error + "\n";
                            }
                        }
                    }
                } catch (IOException e) {
                    response += "Something went wrong and '" + fileName + "' could not be deleted!\n" + e.getMessage()
                            + "\n";
                }
            } else {
                response += "'" + fileName + "' was not deleted since it is a directory\n";
            }
        } else {
            String dirPath = path.replace(fileName, "_" + fileName.replaceAll(".txt", ""));
            File directory = new File(dirPath);
            if (directory.exists()) {
                response += deleteDirectory(directory);
            } else {
                response += "Can't remove '" + fileName + "' it already does not exist in the current directory.\n";
            }
        }

        return response;
    }

    private void onOpenCommand(String message) throws IOException {
        String dirName = message.split("~!~")[1];
        String response;
        if (dirName.equals("..")) {
            int index = this.currentDir.lastIndexOf("/");
            if (index == -1) {
                this.currentDir = "home";
            } else {
                this.currentDir = this.currentDir.substring(0, index);
            }

            response = "change_current_dir" + "~!~" + this.currentDir;
        } else {
            String path = this.currentDir + "/" + dirName;
            File dir = new File(path);
            if (dir.exists()) {
                response = "change_current_dir" + "~!~" + path;
                this.currentDir = path;
            } else {
                response = "message" + "~!~" + "Directoy '" + dirName
                        + "' does not exist in the current directory.";
            }
        }

        send(response);
    }

    private void onListCommand(String message) throws IOException {
        File dir = new File(this.currentDir);
        File[] files = dir.listFiles();
        StringBuilder response = new StringBuilder();
        response.append("message");
        response.append("~!~");
        if (files.length > 0) {
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith("_")) {
                    name = name.substring(1) + ".txt";
                }

                response.append(name);
                response.append(System.getProperty("line.separator"));
            }
        } else {
            response.append("Nothing to list here!");
        }

        send(response.toString());
    }

    private void onMakeCommand(String message) throws IOException {
        String dirName = message.split("~!~")[1];
        String path = this.currentDir + "/" + dirName;
        String response = "message" + "~!~";
        File file = new File(path);
        if (!file.exists()) {
            if (file.mkdir()) {
                response += "Directory '" + dirName + "' was successfully created";
            } else {
                response += "Failed to create directory '" + dirName + "'";
            }
        } else {
            response += "Directory '" + dirName + "' already exists.";
        }

        send(response);
    }

    private void onInitCommand() {
        this.currentDir = "home";
        String response = "message" + "~!~";
        File file = new File("home");
        response += deleteDirectory(file);
        response += "----Initialization completed!----\n";
        Long availableSize = NameServer.getAvailableStorageSize();
        response += "----Available size: " + formatSize(availableSize) + "----";
        send(response);
    }

    private static String formatSize(long v) {
        if (v < 1024)
            return v + " B";
        int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
        return String.format("%.1f %sB", (double) v / (1L << (z * 10)), " KMGTPE".charAt(z));
    }

    private void onRemoveCommand(String message) throws IOException {
        String fileName = message.split("~!~")[1];
        String response = "message" + "~!~";
        String path = this.currentDir + "/" + fileName;
        File file = new File(path);
        response += deleteDirectory(file);
        send(response);
    }

    private String deleteDirectory(File directory) {
        String response = "";
        String directoryName = directory.getName();
        if (directory.exists()) {
            if (directory.isDirectory()) {
                File[] files = directory.listFiles();
                for (File file : files) {
                    if (file.isDirectory()) {
                        response += deleteDirectory(file);
                    } else {
                        response += deleteFile(file);
                    }
                }

                if (!directory.getName().equals("home")) {
                    if (directory.delete()) {
                        response += "'" + directoryName + "' was successfully removed\n";
                    } else {
                        response += "Something went wrong and directory '" + directoryName + "' could not be removed\n";
                    }
                }

                response += "Directory '" + directoryName
                        + "' was successfully removed along with all of its contents\n";
            } else {
                response += "Error: '" + directoryName + "' is NOT a directory\n";
            }
        } else {
            response += "Error: '" + directoryName + "' does not exist\n";
        }

        return response;
    }

    private void registerStorageServer(String message) throws IOException {
        String port = message.split("~!~")[1];
        String address = this.mySocket.getInetAddress().getHostAddress() + ":" + port;
        System.out.println("Added storage server: " + address);
        NameServer.addStorageServer(address, this);
        isStorageServerConnection = true;
        send("success");
    }

    public void successInWritingToStorageServer(String message) throws IOException {
        String[] data = message.split("~!~");
        String address = data[1];
        String path = data[2];
        String localPath = getPath(path);
        String response = "message" + "~!~";
        File file = new File(localPath);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            if (file.createNewFile()) {
                Files.write(Paths.get(localPath), address.getBytes());
                response += "File '" + path + "' was successfully indexed.";
                NameServer.replicaMaker.addToQueue(localPath, address);
            } else {
                response += "Failed to index file '" + path + "'";
            }
        } else {
            response += "Failed to index '" + path + "' because an index already exists";
        }

        send(response);
    }

    public static String getPath(String path){
        String localPath = path;
        int index = path.indexOf("_");
        if(index > -1){
            int indexOfLastSlash = path.lastIndexOf("/");
            String filePath = path.substring(0, indexOfLastSlash);
            String fileName = path.substring(indexOfLastSlash + 1);
            String dirName = "_" + path.substring(indexOfLastSlash + 1, index);
            localPath = filePath + "/" + dirName + "/" + fileName;
        }

        return localPath;
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
            NameServer.removeStorageServer(this);
            isClosed = true;
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




class ReplicaMaker extends Thread {
    private Queue<ReplicaItem> replicaQueue = new LinkedList<ReplicaItem>();

    public void run() {
        while (true) {
            try {
                makeReplicas();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("The replica dealer was interrupted!");
                e.printStackTrace();
            }
        }
    }

    private void makeReplicas() {
        while (!replicaQueue.isEmpty()) {
            ReplicaItem item = replicaQueue.poll();
            String replicaAddress = NameServer.getAvailableStorageServerForWriting(item.getAddress());
            if (replicaAddress != null) {
                NamingServerUser storage = NameServer.getStorageServerByAddress(item.getAddress());
                storage.send("replica_to_storage" + "~!~" + item.getFilePath() + "~!~"
                        + replicaAddress);
                try {
                    String response = storage.in.readUTF();
                    if (response.startsWith("success_replica_write_to_storage_server")) {
                        String[] data = response.split("~!~");
                        String address = data[1];
                        String path = data[2];
                        File file = new File(path);
                        if (file.exists()) {
                            String addresses = Files.readAllLines(Paths.get(path)).get(0);
                            addresses += System.getProperty("line.separator") + address;
                            Files.write(Paths.get(path), addresses.getBytes());
                            System.out.println("Replica of file '" + path + "' was created.");
                            System.out.println("It can now be found on:");
                            System.out.println(addresses);
                        } else {
                            System.out.println("Failed to index the replica of " + path + " on " + address + " as an index does not exist at all.");
                        }
                    } else {
                        System.out.println("Error in creating a replica of file '" + item.getFilePath() + "'.");
                        replicaQueue.add(item);
                        break;
                    }
                } catch (IOException e) {
                    System.out.println("Error in creating a replica of file '" + item.getFilePath() + "'.");
                    replicaQueue.add(item);
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    public void addToQueue(String filePath, String address) {
        ReplicaItem item = new ReplicaItem(filePath, address);
        replicaQueue.add(item);
    }
}



class ReplicaItem {
    private String filePath;
    private String address;

    public ReplicaItem(String filePath, String address){
        this.filePath = filePath;
        this.address = address;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

}



