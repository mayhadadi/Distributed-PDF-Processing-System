package may;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.ec2.model.*;

public class LocalApplication {

    private static String LOCAL_TO_MANAGER;
    private static String MANAGER_TO_WORKER;
    private static String WORKER_TO_MANAGER;
    private static String S3_BUCKET_NAME;
    

    private final AWS aws;

    public LocalApplication() {
        this.aws = AWS.getInstance();
        

        boolean created = false;
        while (!created) {
            S3_BUCKET_NAME = UUID.randomUUID().toString();
            created = aws.createBucketIfNotExists(S3_BUCKET_NAME);
        }
        checkOrStartManagerNode();
    }

    public void job(String inputFileName, String outputFileName, int n, boolean terminate) throws IOException {
        File input_file = new File(inputFileName);
        String s3FileUrl = aws.uploadFileToS3(S3_BUCKET_NAME, inputFileName, input_file);
        System.out.println("uploaded input file to S3.");

        if (terminate){ 
            aws.sendMessageToQueue(LOCAL_TO_MANAGER, s3FileUrl + "\t" + n + "\t" + outputFileName + "\t" + S3_BUCKET_NAME + "\t" + "terminate");
        }
        else{
            aws.sendMessageToQueue(LOCAL_TO_MANAGER, s3FileUrl + "\t" + n + "\t" + outputFileName + "\t" + S3_BUCKET_NAME);

        }

        ReceiveMessageResponse resultFileUrl = waitForResults();
        File tempFile = new File("tempFile_" + S3_BUCKET_NAME + ".txt");
        try {
            if (tempFile.createNewFile()) {
                System.out.println("File created: " + tempFile.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        aws.downloadFileFromS3(S3_BUCKET_NAME, getKeyFromUrl(resultFileUrl.messages().get(0).body()), tempFile);
        System.out.println("downloaded summary file.");
        aws.deleteBucket(S3_BUCKET_NAME);

        convertTextToHtml(tempFile, outputFileName);
        tempFile.delete();

    }

    public void terminateManagerInstance(String managerId) {
        aws.terminateEC2Instance(managerId);
    }

    private void checkOrStartManagerNode() {
        if (!aws.isManagerActive()) {
            System.out.println("Manager node is not active. Starting manager...");
            LOCAL_TO_MANAGER = aws.createQueue("LOCAL_TO_MANAGER");
            MANAGER_TO_WORKER = aws.createQueue("MANAGER_TO_WORKER");
            WORKER_TO_MANAGER = aws.createQueue("WORKER_TO_MANAGER");
            startManagerInstance();
        } else {
            System.out.println("Manager node is active.");
        }
    }

    public String getKeyFromUrl(String url) {
        System.err.println(url);
        String[] urlA = url.split("/");
        System.err.println(urlA[urlA.length - 1]);
        return urlA[urlA.length - 1];
    }

    private void startManagerInstance() {
        String managerId = aws.launchEC2Instance("Manager", 1); 

        if (managerId != null) {
            System.out.println("Started new Manager instance with ID: " + managerId);
            waitForInstanceToBeRunning(managerId);
            System.out.println("Manager instance is now running.");
        }
    }

    public void waitForInstanceToBeRunning(String instanceId) {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        boolean isRunning = false;

        while (!isRunning) {
            try {
                DescribeInstancesResponse response = aws.ec2.describeInstances(describeInstancesRequest);
                Instance instance = response.reservations().get(0).instances().get(0);

                if (instance.state().name() == InstanceStateName.RUNNING) {
                    isRunning = true;
                } else {
                    System.out.println("Waiting for instance " + instanceId + " to be running...");
                    Thread.sleep(5000);
                }
            } catch (Ec2Exception | InterruptedException e) {
                System.err.println("Error while waiting for instance to become running: " + e.getMessage());
                break;
            }
        }
    }

    private ReceiveMessageResponse waitForResults() {
        while (true) {
            String summaryqueue = aws.getQueueUrl(S3_BUCKET_NAME);
            if(summaryqueue != null){
                ReceiveMessageResponse messageBody = aws.receiveMessageFromQueue(summaryqueue);
                if (messageBody != null) {
                    aws.deleteSqs(S3_BUCKET_NAME);
                    return messageBody;
                }
            }
            else{
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void convertTextToHtml(File textFile, String htmlFilePath) throws IOException {
        String plainTextContent = new String(Files.readAllBytes(textFile.toPath()), StandardCharsets.UTF_8);

        String htmlContent = "<html>\n<head>\n<title>Summary</title>\n</head>\n<body>\n<pre>"
                + plainTextContent
                + "</pre>\n</body>\n</html>";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(htmlFilePath))) {
            writer.write(htmlContent);
            System.out.println("HTML file created successfully: " + htmlFilePath);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: java -jar LocalApplication.jar inputFileName outputFileName n [terminate]");
            return;
        }
        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]); //number of lines per worker
        boolean terminate = false;
        if(args.length > 3)
            terminate = args[3].equals("terminate");
        LocalApplication local = new LocalApplication();
        local.job(inputFileName, outputFileName, n, terminate);
    }
}


