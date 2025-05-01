package may;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {

    protected static String LOCAL_TO_MANAGER = "";
    protected static String MANAGER_TO_WORKER = "";
    protected static String WORKER_TO_MANAGER = "";

    private static List<String> workersidList;
    protected static boolean mshouldTerminate;
    protected static String managerId;
        static ConcurrentHashMap<String, String> locals_output_names = new ConcurrentHashMap<String, String>();
        static ConcurrentHashMap<String, Integer> locals_tasks_num = new ConcurrentHashMap<String, Integer>();
        static ConcurrentHashMap<String, List<String>> outputs = new ConcurrentHashMap<String, List<String>>();
        static ConcurrentHashMap<String, Integer> locals_n = new ConcurrentHashMap<String, Integer>();
        private ExecutorService pool;
    
        protected final static AWS aws = AWS.getInstance();
        private static final int MAX_WORKERS_EC2 = 7;
        private static final int MAX_MORKER = 4;
        public static AtomicInteger neededWorkers = new AtomicInteger(0);
        public static AtomicInteger workersCount = new AtomicInteger(0);
        public static AtomicInteger apps_count = new AtomicInteger(0);
    
        public Manager() {
            LOCAL_TO_MANAGER = aws.getQueueUrl("LOCAL_TO_MANAGER");
            MANAGER_TO_WORKER = aws.getQueueUrl("MANAGER_TO_WORKER");
            WORKER_TO_MANAGER = aws.getQueueUrl("WORKER_TO_MANAGER");
            workersCount.set(0);
            apps_count.set(0);
            neededWorkers.set(0);
            mshouldTerminate = false;
            managerId = aws.getManagerInstanceId();
            
            workersidList = new ArrayList<String>();
            this.pool = Executors.newFixedThreadPool(MAX_MORKER);
        }
    
        public static void createWorkers(int n) {
            n = Math.min(n, MAX_WORKERS_EC2 - workersCount.get());
            for (int i = 0; i < n; i++) {
                String Workerid = aws.launchEC2Instance("worker", MAX_WORKERS_EC2);
                workersCount.incrementAndGet();
                workersidList.add(Workerid);
                System.out.println("added worker.");
            }
    
        }
    
        public static void terminateWorker(int n) {
            n = Math.max(n, workersCount.get());
            for (int i = 0; i < n; i++) {
                if (!workersidList.isEmpty()) {
                    aws.terminateEC2Instance(workersidList.remove(0));
                    workersCount.decrementAndGet();
                    System.err.println("worker terminated");
                }
            }
        }
    
        public static void main(String[] args) {
            System.out.println("manager started");
            Manager m = new Manager();
            Sort_Manager sm = new Sort_Manager();
            Thread smt = new Thread(sm);
            smt.start();
    
            while (!mshouldTerminate) {
                m.AcceptNewClientFromQueue();
            }
            
            System.out.println("manager cant receive new clients.");
            while(sm.working){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted while waiting for tasks to complete");
                }
            }
            while(!(Manager.apps_count.get() == 0)){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted while waiting for tasks to complete");
                }
            }
            acceptTerminate();
        
            m.pool.shutdown();
            sm.terminate();
            aws.terminateEC2Instance(managerId);
        }
    
        public String getKeyFromUrl(String url){
            String[] urlA=url.split("/");
            return urlA[urlA.length-1];
        }
    
        public static void acceptTerminate(){
            System.out.println("manager received terminate notice");
            terminateWorker(workersCount.get());
            System.out.println("workers terminated");
            aws.deleteSqs(LOCAL_TO_MANAGER);
            aws.deleteSqs(WORKER_TO_MANAGER);
            aws.deleteSqs(MANAGER_TO_WORKER);
            System.out.println("deleted queues");
        }
    
        public void AcceptNewClientFromQueue() {
            ReceiveMessageResponse input = aws.receiveMessageFromQueue(LOCAL_TO_MANAGER);        
            if (input != null) {
                System.err.println("manager recieved message");
                try {
                    aws.deleteMessageFromQueue(LOCAL_TO_MANAGER, input.messages().get(0).receiptHandle());  
                    String[] task = input.messages().get(0).body().split("\t");
                   
                    if(task.length == 5 && task[4].equals("terminate")){
                        
                        mshouldTerminate = true; 
                        System.out.println("manager detected terminate signal");
                        
                    }
    
                    if (task.length != 4 && task.length != 5) {
                        throw new IOException("message is not in right format");
                    }
                    apps_count.incrementAndGet();
    
                    String txt_url = task[0];
                    int n = Integer.parseInt(task[1]);
                    String outputName = task[2];
                    String localid = task[3];
                    if (Manager.outputs.get(localid) == null)    
                        outputs.put(localid, new ArrayList<String>());
                    locals_n.put(localid, n);
                    locals_output_names.put(localid, outputName);
    
                    File input_File = new File(localid + "_inputfile");
                    aws.downloadFileFromS3(localid , getKeyFromUrl(txt_url) , input_File);
                    System.err.println("manager downloaded input file");
    
                    Morker_Task task2 = new Morker_Task(input_File.getAbsolutePath(), localid);
                    System.err.println("manager created task");
                    pool.submit(task2);
                    System.err.println("manager submitted");
    
                    
    
                } catch (IOException e) {
                    System.err.println("io error");
                    e.printStackTrace();
                }
            } else {
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("manager cant sleep");
                }
    
            }
        }
    
        public static void updateNeeded(int size) {
            Manager.neededWorkers.addAndGet(size);
            if (size >= 0) {
                System.err.println(size);
                createWorkers(size);
            }else{
                terminateWorker(size);
                System.err.println(size);
            }
        }
}
    
    class Morker_Task implements Runnable {
        String file_path;
        String localid;
        AWS aws;
       
        public Morker_Task( String path, String local) {
            this.file_path = path;
            this.localid = local;
            this.aws = AWS.getInstance();
            System.err.println("started morker");  
        }
    
        @Override
        public void run() {
    
            List<String> tasks = readFileToList(file_path);
    
            Manager.locals_tasks_num.put(localid, tasks.size());
            Manager.updateNeeded(tasks.size() / Manager.locals_n.get(localid));
            String mtw = Manager.MANAGER_TO_WORKER;
            System.out.println("morker starts sending requests to workers");
            while (!tasks.isEmpty()) {
                String task = tasks.remove(0);
                task = task + "\t" + localid;
                aws.sendMessageToQueue(mtw, task);
            }     
            System.out.println("morker sent all the requests to the workers");
        }
    
        public List<String> readFileToList(String filePath) {
            List<String> fileContents = new ArrayList<>();
    
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    fileContents.add(line.trim());
                }
            } catch (FileNotFoundException e) {
                System.out.println("Error: The file at " + filePath + " was not found.");
            } catch (IOException e) {
                System.out.println("Error: There was a problem reading the file at " + filePath + ".");
            }
    
            return fileContents;
        }
    }
    
    class Sort_Manager implements Runnable {
    
        String sqsurl;
        AWS aws;
        boolean shouldTerminate;
        boolean working;
    
        public Sort_Manager() {
            this.aws = AWS.getInstance();
            this.sqsurl = aws.getQueueUrl("WORKER_TO_MANAGER");
            this.shouldTerminate = false;
            this.working = true;
            System.out.println("sort manager started.");
        }
    
        public void terminate() {
            this.shouldTerminate = true;   
        }
    
        @Override
        public void run() {
            while (!shouldTerminate) {
                ReceiveMessageResponse output = aws.receiveMessageFromQueue(sqsurl);
                if (output != null) {
                    aws.deleteMessageFromQueue(sqsurl, output.messages().get(0).receiptHandle());
                    String[] splitted=(output.messages().get(0).body().split("\t"));
                    String localid = splitted[splitted.length-1];
                   
                    if (Manager.outputs.get(localid) == null) 
                        Manager.outputs.put(localid, new ArrayList<String>());
    
                    Manager.outputs.get(localid).add(output.messages().get(0).body());
                    
                    if (lastMessage(localid)) {              
                        Runnable sum = new Summary_Manager(localid);
                        Thread summary = new Thread(sum);
                        summary.start();
                        try {
                            summary.join();
                            System.out.println("Summary_Manager finished.");
                        } catch (InterruptedException e) {
                            System.err.println("Interrupted while waiting for Summary_Manager to finish.");
                        }
                        if(Manager.mshouldTerminate)
                        this.working = false;
                    }
             
                }else {
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("sorter cant sleep.");
                }
                }
        }
    }

    private boolean lastMessage(String localid) {
        System.out.println("outputs size: " + Manager.outputs.get(localid).size());
        System.out.println("local tasks num: " + Manager.locals_tasks_num.get(localid));
        if (Manager.outputs.get(localid).size() == Manager.locals_tasks_num.get(localid)) {
            return true;
        }
        return false;

    }
}

class Summary_Manager implements Runnable {
    private final String localId;
    protected boolean working;

    public Summary_Manager(String localId) {
        this.localId = localId;
        System.out.println("summary manager started.");
    }
    @Override
    public void run() {
        working = true;
        try {
            // Fetch results for the localId from Sort_Manager.outputs
            List<String> results = Manager.outputs.get(localId);
            int linesCount = results.size();
            if (linesCount == 0) {
                System.err.println("No results found for localId: " + localId);
                return;
            }

            // Create a summary file
            String outputFileName = localId + "_summary.txt";
            saveResultsToFile(outputFileName, results);

            // Upload file to S3 using Manager's AWS instance 
            String s3FileUrl = Manager.aws.uploadFileToS3(localId, outputFileName, new File(outputFileName));

            // Notify the local application via SQS using Manager's AWS instance
            Manager.aws.createQueue(localId);
            Manager.aws.sendMessageToQueue(Manager.aws.getQueueUrl(localId), s3FileUrl);

            // Remove the localId from the outputs map
            synchronized (Manager.outputs) {
                Manager.outputs.remove(localId);
            }
            synchronized (Manager.locals_output_names) {
                Manager.locals_output_names.remove(localId);
            }
            synchronized (Manager.locals_tasks_num) {
                Manager.locals_tasks_num.remove(localId);
            }
            synchronized (Manager.locals_n) {
                Manager.updateNeeded(0 - (linesCount / Manager.locals_n.get(localId)));
                Manager.locals_n.remove(localId);
            }
            
            System.out.println("Processed and removed localId: " + localId);

            File summaryFile = new File(outputFileName);
            if (summaryFile.delete()) {
                System.out.println("Deleted temporary file: " + outputFileName);
            } else {
                System.err.println("Failed to delete temporary file: " + outputFileName);
            }
            Manager.apps_count.decrementAndGet();
        } catch (Exception e) {
            System.err.println("Exception in Summary_Manager for localId " + localId + ": " + e.getMessage());
            e.printStackTrace();
        }
        
        working = false;
    }

    private void saveResultsToFile(String outputFileName, List<String> results) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
            for (String result : results) {
                writer.write(result);
                writer.newLine();
            }
        }
    }
}
