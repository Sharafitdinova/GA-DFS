import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Client {
    static ClientThread clientThread;
    static Scanner scanner = new Scanner(System.in);
    static String currentDir = "home> ";
    static ConcurrentHashMap<String, File> chunks;
    static String localFilePath = null;
    static boolean Exit = false;
    static String separator = "~!~";

    public static void main(String[] args) {


        System.out.print("Name Server IP: ");
        String host = scanner.nextLine();
        System.out.print("Name Server Port: " );
        int port = Integer.parseInt(scanner.nextLine());


        clientThread = new ClientThread(host, port);


        String Input;
        System.out.println(CommandList());

        do {
            System.out.println(Client.currentDir);
            Input = scanner.nextLine().toLowerCase();
            String[] InputSplit = Input.split(" ");
            LogFile.log("Client input - " + Input);

            if (InputSplit.length > 0) {

                String command = InputSplit[0];
                String request = command;
                if (!command.equals(Input)) {
                    String parameter = Input.substring(command.length() + 1);
                    if (command.equals("nano")) {
                        parameter = InputSplit[1];
                        localFilePath = InputSplit[1];
                        chunk(localFilePath, parameter);
                        continue;


                    } else {
                        localFilePath = null;
                    }

                    request = command + separator + parameter;
                } else if (command.equals("reset")) {

                    currentDir = "home>";
                } else if (command.equals("exit")) {
                    Exit = true;
                    break;
                }

                Client.clientThread.sendMessage(request);
                LogFile.log("Sent to naming server - " + request);
            } else {
                System.err.println("Command NOT Found!");
            }
        } while (!Input.equalsIgnoreCase("exit"));

        scanner.close();
        clientThread.close();
    }

    public static File getChunk(String path) {
        return chunks.get(path);
    }

    public static void removeChunk(String path) {
        File file = chunks.remove(path);
        LogFile.log("Removing chunk - " + file.getAbsolutePath());
        if (file != null && path.contains("_")) {
            file.delete();
        }
    }

    private static void chunk(String localFilePath, String dfsPath) {
        File file = new File(localFilePath);
        chunks = new ConcurrentHashMap<String, File>();
        chunks.put(dfsPath, file);

        for (String path : chunks.keySet()) {
            String request = "nano" + separator + path;
            Client.clientThread.sendMessage(request);
            LogFile.log("Send to naming server - " + request);
        }
    }



    private static ConcurrentHashMap<String, File> splitFile(File file, String dfsPath) throws IOException {
        int counter = 1;
        ConcurrentHashMap<String, File> files = new ConcurrentHashMap<String, File>();
        String eof = System.lineSeparator();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = br.readLine();
            while (line != null) {
                dfsPath = dfsPath.replace(".txt", "");
                String newFileName = dfsPath + "_" + String.format("%03d", counter++)
                        + ".txt";
                File newFile = new File(file.getParent(), newFileName);
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (line != null) {
                        byte[] bytes = (line + eof).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > 1000)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        line = br.readLine();
                    }
                }
                files.put(newFileName, newFile);
            }
        }
        return files;
    }

    private static String CommandList() {
        StringBuilder s = new StringBuilder();
        s.append("\n\n--------------------------------------------------------\n");
        s.append("----------------- Allowed System Calls -----------------\n");
        s.append("--------------------------------------------------------\n\n");
        s.append("nano <LocalFileName> \t- create a new file from a local file\n");
        s.append("cat <FileName> \t\t- read a file\n");
        s.append("rm <FileName> \t\t- delete a file\n");
        s.append("info <FileName> \t- provides information about a file\n");
        s.append("mkdir <DirectoryName> \t- creates a directory\n");
        s.append("cd <DirectoryName> \t- open a directory\n");
        s.append("rmdir <DirectoryName> \t- removes the specified directory\n");
        s.append("ls \t\t\t- returns list of files and directory\n");
        s.append("reset\t\t\t- remove all existed files and print available size\n");
        s.append("exit \t\t\t- to exit the program\n");
        s.append("\n\n------------------------------------------------------\n");
        return s.toString();
    }
}



class ClientThread extends Thread {
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    static String separator = "~!~";

    public ClientThread(String host, int port) {
        try {
            LogFile.log("CLient Thread Started");
            this.socket = new Socket(host, port);
            System.out.println("\n\nConnected To Naming Server");
            this.in = new DataInputStream(this.socket.getInputStream());
            this.out = new DataOutputStream(this.socket.getOutputStream());
            this.start();
        } catch (IOException e) {
            System.err.println("\n***************************************************");
            System.err.println("**************** Failed to Connect ****************");
            System.err.println("***************************************************\n");
            this.close();
        }
    }

    public void run() {
        try {
            LogFile.log("CLient Thread started");
            while (!Client.Exit) {
                String message = this.in.readUTF();
                LogFile.log("Message Received: " + message);
                String[] split = message.split(separator);
                if (split.length >= 2) {
                    String type = split[0];
                    String data = split[1];
                    switch (type) {
                        case "message":
                            System.out.println(data);
                            break;
                        case "change_current_dir":
                            Client.currentDir = data + ">";
                            break;
                        case "read_from_storage_server":
                            readFromStorageServer(message, "read_from_storage_server");
                            break;
                        case "info_from_storage":
                            readFromStorageServer(message, "info_from_storage");
                            break;
                        case "write_from_storage_server":
                            writeToStorageServer(data);
                            break;
                        default:
                            break;
                    }
                } else
                    System.err.println("Error: No Message");


                System.out.println(Client.currentDir);
            }
        } catch (IOException e) {
            this.close();
        }
    }

    void readFromStorageServer(String data, String type) {
        String[] split = data.split(separator);
        StringBuilder content = new StringBuilder();
        boolean success = true;
        for (int i = 1; i < split.length; i++) {
            String[] splitData = split[i].split(" ");
            String address = splitData[0];
            String filePath = splitData[1];
            LogFile.log("Reading from " + address + " file " + filePath);
            String[] splitAddress = address.split(":");
            String ip = splitAddress[0];
            int port = Integer.parseInt(splitAddress[1]);
            try (Socket storageServer = new Socket(ip, port);
                 DataInputStream ssIn = new DataInputStream(storageServer.getInputStream());
                 DataOutputStream ssOut = new DataOutputStream(storageServer.getOutputStream())) {
                String message = type + separator + filePath;
                ssOut.writeUTF(message);
                ssOut.flush();
                String response = ssIn.readUTF();
                String[] splitResponse = response.split(separator);
                String result = splitResponse[0];
                String resData = splitResponse[1];
                if (result.equals("success")) {
                    content.append(resData);
                    content.append(System.getProperty("line.separator"));
                    content.append(System.getProperty("line.separator"));
                } else {
                    System.out.println(
                            "Error: File Not Found\n" + resData);
                    success = false;
                    break;
                }
            } catch (IOException e) {
                LogFile.log(e);
                System.err.println("Error: File NOT Found");
                System.err.println(e.getMessage());
                success = false;
                break;
            }
        }

        if (success) {
            System.out.println("------------------------------------------------------");
            if(type.equals("info_from_storage") && split.length > 2){
                System.out.println("This file is too large so it has been separated to the following chunks:");
            }

            System.out.println(content.toString());
            System.out.println("------------------------------------------------------");
        }
    }

    void writeToStorageServer(String data) {
        String[] splitData = data.split(" ");
        String address = splitData[0];
        String filePath = splitData[1];
        String[] splitAddress = address.split(":");
        LogFile.log("Writing to " + address + " file " + filePath);
        String ip = splitAddress[0];
        int port = Integer.parseInt(splitAddress[1]);
        String chunkKey = filePath.substring(filePath.lastIndexOf("/") + 1);
        try (Socket storageServer = new Socket(ip, port);
             DataInputStream ssIn = new DataInputStream(storageServer.getInputStream());
             DataOutputStream ssOut = new DataOutputStream(storageServer.getOutputStream())) {
            File file = Client.getChunk(chunkKey);
            StringBuilder contents = new StringBuilder();
            boolean isChunk = true;
            if (file == null) {
                file = new File(Client.localFilePath);
                isChunk = false;
            }

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    contents.append(line);
                    contents.append(System.getProperty("line.separator"));
                }
            }catch (IOException e) {
                LogFile.log(e);
            }

            String remoteFilePath = getPath(filePath);

            String message = "write_from_storage_server" + separator + remoteFilePath + separator
                    + contents.toString();
            ssOut.writeUTF(message);
            ssOut.flush();
            String response = ssIn.readUTF();
            String[] splitResponse = response.split(separator);
            String result = splitResponse[0];
            String resData = splitResponse[1];
            if (result.equals("success")) {
                System.out.println(resData);
                String namingServerMessage = "success_write_to_storage_server" + separator + address
                        + separator + filePath;
                sendMessage(namingServerMessage);
            }
            else
                System.out.println("Error: File NOT Saved!\n" + resData);


            if(isChunk)
                Client.removeChunk(chunkKey);

        }
        catch (IOException e) {
            LogFile.log(e);
            System.err.println("Error: File NOT Found");
            System.err.println(e.getMessage());
        }
    }

    void sendMessage(String message) {
        try {
            LogFile.log("Sending message: " + message);
            this.out.writeUTF(message);
            this.out.flush();
        }
        catch (IOException e) {
            System.err.println("Error: Message NOT Sent");
            this.close();
        }
        catch (NullPointerException e2) {
            System.err.println("Error: Server NOT Connected");
        }
    }

    void close() {
        try {
            if (this.in != null)
                in.close();
            if (this.out != null)
                out.close();
            if (this.socket != null)
                socket.close();
        } catch (IOException e) {
        } finally {
            this.interrupt();
        }
    }

    String getPath(String path){
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

}


class LogFile {
    public static void log(String data){
        File file = new File("log.txt");

        if (!file.exists())
            try {
                file.createNewFile();
            }
            catch (IOException e) {}

        try(BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true))) {
            String date = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
                    .format(new Date());
            bw.write(date + ": " + data + System.getProperty("line.separator"));
        } catch (IOException e) {}
    }

    public static void log(Exception ex){
        log(ex.getMessage());
    }
}
