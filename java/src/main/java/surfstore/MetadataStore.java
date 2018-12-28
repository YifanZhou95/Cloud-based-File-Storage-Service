package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.LogEntry;
import surfstore.SurfStoreBasic.ResultType;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config, port))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
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
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        private boolean debug;
        
        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        protected Hashtable file2hashMap; // added a data structure
        protected Hashtable file2versMap; // added a data structure

        private boolean leaderFlag;
        private boolean crashFlag;
        private int leaderCommitIndex;
        private int localCommitIndex;
        private List<LogEntry> log;
        private List<MetadataStoreGrpc.MetadataStoreBlockingStub> followers;

        public MetadataStoreImpl(ConfigReader config, int port) {

            debug = false;

            file2hashMap = new Hashtable();
            file2versMap = new Hashtable();

            log = new ArrayList<LogEntry>();
            followers = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
            leaderCommitIndex = -1;
            localCommitIndex = -1;
            crashFlag = false;
            leaderFlag = false;
            if(config.getMetadataPort(config.getLeaderNum()) == port) leaderFlag = true;

            blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                    .usePlaintext(true).build();
            blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            
            if (leaderFlag) {
                if(debug) System.out.println("$ leader $: This is leader");
                ManagedChannel metadataChannel;
                MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
                for(int i=1; i<=config.getNumMetadataServers(); i++) {
                    int followerPort = config.getMetadataPort(i);
                    if(config.getMetadataPort(config.getLeaderNum()) != followerPort) {
                        metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", followerPort)
                                .usePlaintext(true).build();
                        metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
                        followers.add(metadataStub);
                    }
                }

                // broadcast heatbeat every half second
                Timer broadcast = new Timer();
                broadcast.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        if(debug) System.out.println("$ send heatbeat $");
                        heatBeat();
                    }
                }, 0, 500);

                // // test for crashed follower
                // // restore after a while
                // if(debug && followers.size()>0) {
                //     Timer crashTest = new Timer();
                //     crashTest.scheduleAtFixedRate(new TimerTask() {
                //         @Override
                //         public void run() {
                //             if(debug) System.out.println("$ test crash $: Crash follower 0");
                //             followers.get(0).crash(Empty.newBuilder().build());
                //             if(debug) System.out.println("$ test restore $: Restore follower 1");
                //             followers.get(1).restore(Empty.newBuilder().build());
                //         }
                //     }, 20000, 20000);
                //     Timer restoreTest = new Timer();
                //     restoreTest.scheduleAtFixedRate(new TimerTask() {
                //         @Override
                //         public void run() {
                //             if(debug) System.out.println("$ test restore $: Restore follower 0");
                //             followers.get(0).restore(Empty.newBuilder().build());
                //             if(debug) System.out.println("$ test crash $: Crash follower 1");
                //             followers.get(1).crash(Empty.newBuilder().build());
                //         }
                //     }, 30000, 20000);
                // }
            }
        }

        // implement background heatbeat process
        private void heatBeat() {
            // create new thread to different follower
            final List<Timer> timerList = new ArrayList<Timer>();
            
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            entryBuilder.setIndex(leaderCommitIndex);
            final LogEntry lastCommitEntry = entryBuilder.build();

            for(MetadataStoreGrpc.MetadataStoreBlockingStub follower : followers) {
                final MetadataStoreGrpc.MetadataStoreBlockingStub follower_const = follower;
                timerList.add(new Timer());
                // broadcast heatbeat every half second
                Timer timer = timerList.get(timerList.size()-1);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        updateLog(follower_const);
                        follower_const.executeRemainingEntries(lastCommitEntry);
                    }
                }, 0);
            }
        }
        
        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // check file is in database (either at present or in the past) or not
        private boolean isExist(String filename) {
            if (file2versMap.containsKey(filename)) return true;
            else return false;
        }

        // get the version of file in database if exists, otherwise return 0
        private int readVersion(String filename) {
            if (isExist(filename)) return (int)file2versMap.get(filename);
            else return 0;
        }

        // get the hashlist of file in database if exists, otherwise return empty list
        private List<String> readHashlist(String filename) {
            if (isExist(filename)) return (List<String>)file2hashMap.get(filename);
            else return new ArrayList<String>();
        }

        @Override
        public synchronized void readFile(FileInfo req, final StreamObserver<FileInfo> responseObserver) {
            String filename = req.getFilename();
            if(debug) System.out.println("Received filename is: " + filename);

            int version = readVersion(filename);
            List<String> hashlist = readHashlist(filename);

            FileInfo.Builder responseBuilder = FileInfo.newBuilder();
            responseBuilder.setFilename(filename);
            responseBuilder.setVersion(version);
            for(String hash : hashlist) responseBuilder.addBlocklist(hash);
            FileInfo response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // call rpc:hasBlock() to search missing blocks
        private List<String> queryBlocks(List<String> querylist) {
            List<String> residuelist = new ArrayList<String>();
            for(String query : querylist) {
                Block.Builder requestBuilder = Block.newBuilder();
                requestBuilder.setHash(query);
                Block request = requestBuilder.build();
                SimpleAnswer response = blockStub.hasBlock(request);
                if (!response.getAnswer()) residuelist.add(query);
            }
            unique(residuelist);
            return residuelist;
        }

        private void unique(List<String> list)
        {
            HashSet set = new HashSet(list);
            list.clear();
            list.addAll(set);
        }

        @Override
        public synchronized void modifyFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) {
            String filename = req.getFilename();
            List<String> hashlist = req.getBlocklistList();
            int version = req.getVersion();
            int current_version = readVersion(filename);

            List<String> hashlistMissing = new ArrayList<String>();
            int result = -1;  // initialize as -1 tag
            if(debug) System.out.println("Received filename to be modified is: " + filename);
            if(debug) System.out.println("Current version is: " + current_version);
            if(!leaderFlag){
                if(debug) System.out.println("Not leader warning");
                result = 3;  // result=3 indicates not leader
            } else if(version != current_version + 1) {
                if(debug) System.out.println("File version conflicts.");
                result = 1;  // result=1 indicates incorrect version
            } else {
                hashlistMissing = queryBlocks(hashlist);
                if(debug) System.out.println("Number of missing blocks: " + hashlistMissing.size());
                if(hashlistMissing.size() > 0) {
                    result = 2;  // result=2 indicates non-zero missing blocks
                    if(debug) System.out.println("File has missing blocks");
                } else {
                    // prepare input for two-phase-commit
                    if(debug) System.out.println("Prepare two phase commit...");
                    LogEntry.Builder entryBuilder = LogEntry.newBuilder();
                    entryBuilder.setIndex(log.size());
                    entryBuilder.setOperation(new String("modify"));
                    entryBuilder.setContent(req);
                    LogEntry entry = entryBuilder.build();
                    // call method
                    result = twoPhaseCommit(entry);  // result=0 indicates metadataStore saves the mapping
                    if(debug) System.out.println("File has been modified");
                }
            }
            
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();
            responseBuilder.setResultValue(result);
            responseBuilder.setCurrentVersion(readVersion(filename));
            for(String hash : hashlistMissing) responseBuilder.addMissingBlocks(hash);
            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // File version + 1 once deleted successfully. But what if multiple delete happens?
        @Override
        public synchronized void deleteFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) {
            String filename = req.getFilename();
            int version = req.getVersion();
            int current_version = readVersion(filename);

            int result = -1;  // initialize as -1 tag
            if(debug) System.out.println("Received filename to be deleted is: " + filename);
            if(debug) System.out.println("Current version is: " + current_version);
            if(!leaderFlag){
                if(debug) System.out.println("Not leader warning");
                result = 3;  // result=3 indicates not leader
            } else if(current_version == 0) {
                if(debug) System.out.println("File never exists.");
                result = 2;  // result=2 indicates file never exists
            } else if(version != current_version + 1) {
                if(debug) System.out.println("File version conflicts.");
                result = 1;  // result=1 indicates incorrect version
            } else {
                // prepare input for two-phase-commit
                if(debug) System.out.println("Prepare two phase commit...");
                LogEntry.Builder entryBuilder = LogEntry.newBuilder();
                entryBuilder.setIndex(log.size());
                entryBuilder.setOperation(new String("delete"));
                entryBuilder.setContent(req);
                LogEntry entry = entryBuilder.build();
                // call method
                result = twoPhaseCommit(entry);  // result=0 indicates metadataStore saves the mapping
                if(debug) System.out.println("File is deleted.");
            }
            
            WriteResult.Builder responseBuilder = WriteResult.newBuilder();
            responseBuilder.setResultValue(result);
            responseBuilder.setCurrentVersion(readVersion(filename));
            WriteResult response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(FileInfo req, final StreamObserver<FileInfo> responseObserver) {
            String filename = req.getFilename();
            int current_version = readVersion(filename);
            FileInfo.Builder responseBuilder = FileInfo.newBuilder();
            responseBuilder.setFilename(filename);
            responseBuilder.setVersion(current_version);
            FileInfo response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isLeader(Empty req, final StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
            responseBuilder.setAnswer(leaderFlag);
            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(Empty req, final StreamObserver<SimpleAnswer> responseObserver) {
            if(debug && crashFlag) System.out.println("Server crashed :(");
            SimpleAnswer.Builder responseBuilder = SimpleAnswer.newBuilder();
            responseBuilder.setAnswer(crashFlag);
            SimpleAnswer response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        
        @Override
        public void crash(Empty req, final StreamObserver<Empty> responseObserver) {
            boolean oldFlag = crashFlag;
            crashFlag = true;
            if(debug && (oldFlag^crashFlag)) System.out.println("Server turns to be crashing :(");
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        
        @Override
        public void restore(Empty req, final StreamObserver<Empty> responseObserver) {
            boolean oldFlag = crashFlag;
            crashFlag = false;
            if(debug && (oldFlag^crashFlag)) System.out.println("Server becomes available :)");
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // check file is in database (either at present or in the past) or not
        private int twoPhaseCommit(LogEntry entry) {
            // local log append
            int index = log.size();
            log.add(entry);

            // phase 1: leader calls votes from followers
            int voteCount = 1;  // leader never fails
            ResultType appendResponse;
            for(MetadataStoreGrpc.MetadataStoreBlockingStub follower : followers) {
                // Assume all followers are online, otherwise leader would crash
                // try to append new to follower; fail if follower crashes or out-of-date
                appendResponse = follower.appendEntries(entry);
                if(appendResponse.getResultValue()==0) {
                    voteCount += 1;
                }
            }
            if(debug) System.out.println("Phase 1: Vote result: " + voteCount);

            // phase 2: commit if number of votes exceeds half
            if(voteCount > (0.5+0.5*followers.size())) {
                if(debug) System.out.println("Phase 2: Vote succeed, commit!");
                // execute this entry locally...
                int result = commit(entry);
                leaderCommitIndex = index;
                // broadcast executing this entry to followers...
                // for(MetadataStoreGrpc.MetadataStoreBlockingStub follower : followers) {
                //     // skip crashed followers
                //     // SimpleAnswer answer = follower.isCrashed(Empty.newBuilder().build());
                //     ResultType response = follower.executeRemainingEntries(entry);
                //     result = response.getResultValue();  // should be 0 if succeed
                // }
                return result;
            } else {
                if(debug) System.out.println("Phase 2: Vote failed, abort!");
                return 3;  // indicates this command can not be executed
            }
        }

        private int updateLog(MetadataStoreGrpc.MetadataStoreBlockingStub follower) {
            int i;
            // query leader's log
            for(i=log.size()-1; i>=0; i--) {
                LogEntry entry = log.get(i);
                ResultType response = follower.appendEntries(entry);
                if(i==log.size()-1 && response.getResultValue()==3) return 1;
                if(response.getResultValue()==1) return 1;
                if(response.getResultValue()==0) break;
            }
            // update local log
            for(i=i+1; i<=log.size()-1; i++) {
                LogEntry entry = log.get(i);
                ResultType response = follower.appendEntries(entry);
            }
            return 0;
        }

        @Override
        public void appendEntries(LogEntry req, final StreamObserver<ResultType> responseObserver) {
            ResultType.Builder responseBuilder = ResultType.newBuilder();
            if(crashFlag) {
                if(debug) System.out.println("Server crashed :(");
                responseBuilder.setResultValue(1);
            } else {
                if((log.size()==0 && req.getIndex()==0) || (log.size()>0 && log.get(log.size()-1).getIndex()+1==req.getIndex())) {
                    log.add(req);
                    responseBuilder.setResultValue(0);
                    if(debug) System.out.println("Log append valid");
                } else if(log.size()>0 && log.get(log.size()-1).getIndex()==req.getIndex()) {
                    responseBuilder.setResultValue(3);
                    if(debug) System.out.println("Log is already up-to-date");
                } else {
                    responseBuilder.setResultValue(2);
                    if(debug) System.out.println("Log append invalid");
                }
            }

            ResultType response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void executeRemainingEntries(LogEntry leaderEntry, final StreamObserver<ResultType> responseObserver) {
            // leaderEntry conveys leaderCommitIndex
            int result = 1;
            if(crashFlag) result = 1;
            else if(leaderEntry.getIndex()<0 || leaderEntry.getIndex()==localCommitIndex) result = 0;
            else {
                int leaderIndex = leaderEntry.getIndex();
                // locate where is last committed entry in local log
                int position = localCommitIndex + 1;
                // commit sequentially
                LogEntry entry;
                for( ; position<log.size(); position++) {
                    entry = log.get(position);
                    result = commit(entry);
                    if(entry.getIndex()==leaderIndex) break;
                }
            }
            ResultType.Builder responseBuilder = ResultType.newBuilder();
            responseBuilder.setResultValue(result);
            ResultType response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void executeLastEntry(LogEntry entry, final StreamObserver<ResultType> responseObserver) {
            int result;
            if(crashFlag) result = -1;
            else result = commit(entry);
            ResultType.Builder responseBuilder = ResultType.newBuilder();
            responseBuilder.setResultValue(result);
            ResultType response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private int commit(LogEntry entry) {
            int index = entry.getIndex();
            String operation = entry.getOperation();
            FileInfo content = entry.getContent();
            String filename = content.getFilename();
            List<String> hashlist = content.getBlocklistList();
            int version = content.getVersion();

            if(debug) System.out.println("Commit! -- index:" + index + ", command: " + operation);
            localCommitIndex = index;

            if(operation.equals("modify")) {
                file2hashMap.put(filename, hashlist);
                file2versMap.put(filename, version);
                return 0;  // result=0 indicates metadataStore saves the mapping
            } else if(operation.equals("delete")) {
                List<String> zero_list = new ArrayList<String>();
                zero_list.add(new String("0"));
                file2hashMap.put(filename, zero_list);
                file2versMap.put(filename, version);
                return 0;  // result=0 indicates metadataStore saves the mapping
            } else return -1;
        }
    }
}