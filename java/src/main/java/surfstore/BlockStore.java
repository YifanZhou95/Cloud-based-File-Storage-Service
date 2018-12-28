package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.SimpleAnswer;


public final class BlockStore {
    private static final Logger logger = Logger.getLogger(BlockStore.class.getName());

    protected Server server;
	protected ConfigReader config;


    public BlockStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BlockStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("BlockStore").build()
                .description("BlockStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        final BlockStore server = new BlockStore(config);
        server.start(config.getBlockPort(), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class BlockStoreImpl extends BlockStoreGrpc.BlockStoreImplBase {

        //private final List<Block> blocks = Collections.synchronizedList(new ArrayList<Block>());
        protected Hashtable blockMap;
        public BlockStoreImpl()
        {
            blockMap = new Hashtable(); // add a new variable
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!
        //rpc StoreBlock(Block) returns(Empty) {}
        //The client must fill both fields of the message
        //storeBlock(h, b): Stores block b in the key-value store, indexed by hash value h
        @Override
        public void storeBlock(Block req, final StreamObserver<Empty> responseObserver)
        {
            String encoded = hashBlock(req);
            // System.out.println("Received block to store:" + encoded);
            Block.Builder blockBuilder = Block.newBuilder();

            if(blockMap.get(encoded) == null)
            {
                blockBuilder.setHash(encoded);
                blockBuilder.setData(req.getData());
                Block blk = blockBuilder.build();   
                blockMap.put(encoded, blk);     
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private String hashBlock(Block blk)
        {
            byte[] blockContent = blk.getData().toByteArray();
            MessageDigest digest = null;
            try{
                digest = MessageDigest.getInstance("SHA-256");
            }
            catch(NoSuchAlgorithmException e)
            {
                e.printStackTrace();
                System.exit(2);
            }
            byte[] hash = digest.digest(blockContent);
            String encoded = Base64.getEncoder().encodeToString(hash);
            return encoded;
        }

        //rpc getBlock(Block) returns (Block) {}
        //Get a block in storage
        //The client only needs to supply the "hash" field
        //The server returns both the "hash" and "data" fields
        //If the block doesn't exist, "hash" will be the empty string
        //We will not call this rpc if the block doesn't exist(we'll always
        //cal "HasBlock()" first)
        @Override
        public void getBlock(Block req, final StreamObserver<Block> responseObserver)
        {
            String encoded = req.getHash();
            Block.Builder blockBuilder = Block.newBuilder();
            blockBuilder.setHash(encoded);
            Block queryBlock = (Block)blockMap.get(encoded);
            blockBuilder.setData(queryBlock.getData());  // (com.google.protobuf.ByteString) ?
            Block response = blockBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        //Check whether a block is in storage.
        //The client only needs to specify the "hash" field.
        @Override
        public void hasBlock(Block req, final StreamObserver<SimpleAnswer> responseObserver)
        {
            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
            if(blockMap.get(req.getHash()) != null)
            {
                responseBuilder.setAnswer(true);
            }
            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }   

    }
}