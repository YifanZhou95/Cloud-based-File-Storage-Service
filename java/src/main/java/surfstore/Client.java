package surfstore;

import java.io.File;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Scanner;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Base64;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.*;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.FileInfo;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    //additional variables
    private final int blockSize;
    private final int numOfCluster;
    private final boolean b_Distributed;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        //addtional initialization
        this.blockSize = 4 * 1024;
        this.numOfCluster = config.getNumMetadataServers();
        if(this.numOfCluster <= 1)
            b_Distributed = false;
        else
            b_Distributed = true;

    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static String sha256(String text)
    {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash); 
        return encoded;      
    }

	private void go() {
		metadataStub.ping(Empty.newBuilder().build());
        // logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        // logger.info("Successfully pinged the Blockstore server");   
        
	}

	/*
	 * TODO: Add command line handling here
	 */
    //create block
    private static Block createMsgBlock(String content, String hashValue)
    {
        Builder builder = Block.newBuilder();
        try{
            builder.setData(ByteString.copyFrom(content, "UTF-8"));
        }catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
        builder.setHash(hashValue);
        return builder.build();
    }
    //create file
    private static FileInfo createFile( ArrayList<String> blockList, String fileName, int version)
    {
        FileInfo.Builder fiBuilder = FileInfo.newBuilder();       
        fiBuilder.setVersion(version);
        fiBuilder.setFilename(fileName);
        if(blockList != null)
            fiBuilder.addAllBlocklist(blockList);
        //FileInfo fInfo = fiBuilder.build();
        return fiBuilder.build();
    }

    //check whether the file has already exist
    /*private boolean isFileExist(String fileName)
    {
        FileInfo req = createFile(null, fileName, -1);
        FileInfo fInfo = metadataStub.readFile(req);
        if(fInfo.getVersion() == 0 || 
            (fInfo.getBlocklistList().size() == 1 && fInfo.getBlocklistList().get(0)=="0"))
        {
            return false;
        }
        return true;
    }*/

    /*private boolean chkValidationOfServer(WriteResult response)
    {
        if(response.getResultValue()!=3){
            return true;
        }
        else{
            return false;
        }
    }*/

    //write file Info
    private void addFile2List(ArrayList<String> hashList, ArrayList<String> fileList, String content)
    {

        int pointer1 = blockSize, pointer2 = 0, length = content.length();      
        while(true){
            String blkContent = "";
            if(pointer1 <= length)
                blkContent = content.substring(pointer2, pointer1);
            else
                blkContent = content.substring(pointer2);
            pointer1 += blockSize; pointer2 += blockSize;
            fileList.add(blkContent);
            String encoded = sha256(blkContent);
            hashList.add(encoded);
            if(pointer2 >= length)
                break;
        }
    }

    private void uploadProcess(String name, ArrayList<String> hashList, ArrayList<String> fileList){
        FileInfo fInfo;
        WriteResult wr;
        while(true){
            FileInfo info = metadataStub.readFile(createFile(null, name, -1));
            int version = info.getVersion();
            fInfo = createFile(hashList, name, version+1);
            wr = metadataStub.modifyFile(fInfo);
            if(wr.getResultValue()==2){
                for(String missBlock : wr.getMissingBlocksList()){
                    int key = hashList.indexOf(missBlock);
                    Block blk = createMsgBlock(fileList.get(key), hashList.get(key));
                    blockStub.storeBlock(blk);
                }
            }
            if(wr.getResultValue() == 3)
                return;
            if(wr.getResultValue()==0)
                break;
        }

        if(wr.getResultValue()==0) System.out.println("OK");
    }


    private void uploadFile(String fileName, String name) throws IOException{
        
        File localFile = new File(fileName);
        if(localFile.exists() == false){
            throw new FileNotFoundException(localFile.getPath());
        }

        String fileContent = new String(Files.readAllBytes(localFile.toPath()), "UTF-8");
        ArrayList<String> hashList = new ArrayList<>(), fileList = new ArrayList<>();
        addFile2List(hashList, fileList, fileContent);
        uploadProcess(name, hashList, fileList);

       /* FileInfo fInfo;
        WriteResult wr;

        while(true){
            FileInfo read = createFile(null, name, -1);
            FileInfo readResult = metadataStub.readFile(read);
            int version = readResult.getVersion();
            fInfo = createFile(hashList, name, version+1);
            wr = metadataStub.modifyFile(fInfo);
            if(wr.getResultValue()==3)
                return;
            else if(wr.getResultValue()==2){
                for(String missing : wr.getMissingBlocksList()){
                    int idx = hashList.indexOf(missing);
                    blockStub.storeBlock(createMsgBlock(fileList.get(idx), hashList.get(idx)));
                }
            }
            if(wr.getResultValue()!=0)
                break;
        }*/

        /*FileInfo read = createFile(null, name, -1);
        FileInfo readResult = metadataStub.readFile(read);
        int version = readResult.getVersion();*/
    }

    private void parseFile2blkMap(File[] files, HashMap<String, String> blkMap) throws IOException{
        for(File file : files){
            String content = new String(Files.readAllBytes(file.toPath()), "UTF-8");
            int length = content.length(), pointer1 = blockSize, pointer2 = 0;       
            while(true){
                String blkContent = "";
                if(pointer1 > length)
                    blkContent = content.substring(pointer2);
                else
                    blkContent = content.substring(pointer2, pointer1);
                pointer1 += blockSize; pointer2 += blockSize;
                blkMap.put(sha256(blkContent), blkContent);
                if(pointer2 >= length)
                    break;
            }
        }
    }
    private ArrayList<String> createFileList(FileInfo fileInfo, HashMap<String, String> blkMap)
    {
        ArrayList<String> fileList = new ArrayList<>();
        int length = fileInfo.getBlocklistCount();
        for(int i=0; i<fileInfo.getBlocklistCount(); i++){
            String blkList = fileInfo.getBlocklist(i);
            if(blkMap.get(blkList) != null){
                fileList.add(blkMap.get(blkList));
            }
            else{
                Block missingBlk = createMsgBlock("null", blkList);
                //Block checkResult = ;
                fileList.add(blockStub.getBlock(missingBlk).getData().toStringUtf8());
            }
        }
        return fileList;
    }
    private void downloadFile(String path, String fileName) throws IOException{
        if(path.lastIndexOf("/") + 1 != path.length()){
            path += "/";
        }

        FileInfo readInfo = metadataStub.readFile(createFile(null, fileName, -1));
        String completePath = path + fileName;

        /*if(readInfo.getVersion()==0 || 
            (readInfo.getBlocklistCount()==1 && readInfo.getBlocklist(0).equals("0"))){
            System.out.println("File Not Exist");
        }*/
        if(readInfo.getVersion()!=0 && 
            (readInfo.getBlocklistCount()!=1 || !readInfo.getBlocklist(0).equals("0")))
        {
            File files = new File(path);
            // make directory
            if(!files.exists()) files.mkdir();
            File[] fileArry = files.listFiles();

            HashMap<String, String> blkMap = new HashMap<String, String>();
            parseFile2blkMap(fileArry, blkMap);

            ArrayList<String> fileList = createFileList(readInfo, blkMap);
            //ArrayList<String> fileList = new ArrayList<>();
           /*ArrayList<String> fileList = new ArrayList<>();
            int length = readInfo.getBlocklistCount();
            for(int i=0; i<readInfo.getBlocklistCount(); i++){
                String blockList = readInfo.getBlocklist(i);
                if(blkMap.containsKey(blockList)){
                    fileList.add(blkMap.get(blockList));
                }
                else{
                    Block missingBlock = createMsgBlock("null", blockList);
                    Block checkResult = blockStub.getBlock(missingBlock);
                    fileList.add(checkResult.getData().toStringUtf8());
                }
            }*/
            //write(writeList , completePath);
            File f = new File(completePath);
            try{
                if(!f.exists()){
                    f.createNewFile();
                }
                FileWriter fWriter = new FileWriter(completePath);
                for(String content : fileList){
                    fWriter.write(content);
                }
                fWriter.close();
                System.out.println("OK");
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
        else{
            System.out.println("Not Found");
        }
    }

    private void deleteFile(String fileName)
    {
        FileInfo readInfo = metadataStub.readFile(createFile(null, fileName, -1));
        int version = readInfo.getVersion();
        WriteResult wr = metadataStub.deleteFile(createFile(null, fileName, version+1));
        if(wr.getResultValue()==0) System.out.println("OK");
        else System.out.println("Not Found");
    }

    private int getVersion(String fileName){
        FileInfo req = metadataStub.getVersion(createFile(null, fileName, -1));
        if(b_Distributed){
            for(int i=0; i<numOfCluster; i++){
                System.out.print(req.getVersion() + " ");
            }
            System.out.println();
        }
        else{
            System.out.print(req.getVersion());
        }
        return req.getVersion();
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("instruction");
        parser.addArgument("file").type(String.class);
        parser.addArgument("option_path").nargs("*");
        
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        String op = c_args.getString("instruction");
        String fileName = c_args.getString("file");
        //String filePath = getFilePath(fileName);
        //get FilePath
        String filePath = "";
        int idx = fileName.lastIndexOf("/");
        if(idx == -1)
            filePath = fileName;
        else
            filePath = fileName.substring(idx+1);

        Client client = new Client(config);
        if(op.equals("upload")){
            client.uploadFile(fileName, filePath);
        }
        else if(op.equals("delete")){
            client.deleteFile(filePath);
        }        
        else if(op.equals("download")){
            String option_path = c_args.getString("option_path");
            option_path = option_path.substring(1, option_path.length()-1);
            client.downloadFile(option_path, filePath);
        }
        else if(op.equals("getversion")){
            client.getVersion(filePath);
        }       
        try {
        	client.go();
        } finally {
            client.shutdown();
        }
    }

}
