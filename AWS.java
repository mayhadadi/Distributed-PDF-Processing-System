package may;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

public class AWS {
    private static final String ManagerScript = "#!/bin/bash\n" +
            "yum update -y\n" +
            "yum install -y aws-cli\n" +
            "aws s3 cp s3://315115261208393074/manager.jar /home/manager.jar\n" +
            "java -jar /home/manager.jar manager \n";

    private static final String WorkerScript = "#!/bin/bash\n" +
            "yum update -y\n" +
            "yum install -y aws-cli\n" +
            "aws s3 cp s3://315115261208393074/worker.jar /home/worker.jar\n" +
            "java -jar /home/worker.jar worker\n";

    private final S3Client s3;
    private final SqsClient sqs;
    protected final Ec2Client ec2;
    private final Region region = Region.US_WEST_2; // Replace with your preferred region
    private static AWS instance = null;
    // private boolean isManagerRunning;
    public static String amiId = "ami-00e95a9222311e8ed";

    private AWS() {
        this.s3 = S3Client.builder().region(region).build();
        this.sqs = SqsClient.builder().region(region).build();
        this.ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
    }

    public static AWS getInstance() {
        if (instance == null) {
            instance = new AWS();
        }
        return instance;
    }

    public synchronized boolean isManagerActive() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if ((instance.state().name() == InstanceStateName.RUNNING
                            || instance.state().name() == InstanceStateName.PENDING) && tag.key().equals("Name")
                            && tag.value().equals("Manager")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    //////////////////////////////// S3 Methods ////////////////////////////////

    public boolean createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.printf("bucket created - %s\n", bucketName);
            return true;
        } catch (S3Exception e) {

            System.out.println(e.getMessage());
            return false;
        }
    }

    public void deleteBucket(String bucket) {
        emptyBucket(bucket);
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                                                    .bucket(bucket)
                                                    .build();
        s3.deleteBucket(deleteBucketRequest);
    }

public void emptyBucket(String bucket) {
    try {
        // List all objects in the bucket
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                                                                      .bucket(bucket)
                                                                      .build();
        ListObjectsV2Response listObjectsResponse;

        do {
            // Get a batch of objects
            listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

            // Delete each object in the bucket
            for (S3Object object : listObjectsResponse.contents()) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                                                             .bucket(bucket)
                                                                             .key(object.key())
                                                                             .build();
                s3.deleteObject(deleteObjectRequest);
            }

            // Prepare the next request if there are more objects to delete
            listObjectsRequest = ListObjectsV2Request.builder()
                                                     .bucket(bucket)
                                                     .continuationToken(listObjectsResponse.nextContinuationToken())
                                                     .build();
        } while (listObjectsResponse.isTruncated());

        System.out.println("Bucket emptied: " + bucket);

    } catch (S3Exception e) {
        System.err.println("Error emptying bucket: " + e.awsErrorDetails().errorMessage());
        throw e;
    }
}


    public String uploadFileToS3(String bucketName, String key, File file) {
        try {
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(),
                    RequestBody.fromFile(file));

            String fileUrl = String.format("https://%s.s3.amazonaws.com/%s", bucketName, key);

            System.out.println("File uploaded to S3: " + fileUrl);
            return fileUrl;

        } catch (S3Exception e) {
            System.err.println("Error uploading file: " + e.getMessage());
            throw e;
        }
    }

    public void downloadFileFromS3(String bucketName, String key, File destination) {
        try {
            byte[] fileBytes = s3.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()).asByteArray();
            try (OutputStream os = new FileOutputStream(destination)) {
                os.write(fileBytes);
            }
            System.out.println("File downloaded from S3: " + key);
        } catch (Exception e) {
            System.err.println("Error downloading file: " + e.getMessage());
        }
    }

    public BufferedReader downloadFile(String bucketName, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            ResponseInputStream<GetObjectResponse> inputStream = s3.getObject(getObjectRequest);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            return reader;
        } catch (Exception e) {
            System.err.printf("Error downloading file from S3: %s\n", e.getMessage());
            throw e;
        }

    }

    //////////////////////////////// SQS Methods ////////////////////////////////

    public String getQueueUrl(String queueName) {
        while (true) {
            try {
                // Get the URL of the SQS queue by tag
                GetQueueUrlResponse queueUrlResponse = sqs.getQueueUrl(
                        GetQueueUrlRequest.builder().queueName(queueName).build());
                // If the queue exists, print its URL and break out of the loop
                System.out.println("Queue URL: " + queueUrlResponse.queueUrl());

                return queueUrlResponse.queueUrl();

            } catch (QueueDoesNotExistException e) {
                // If the queue does not exist, handle the exception
                System.out.println("Queue with this name does not exist. Retrying...");
            }

            // Wait for a specified interval before checking again
            try {
                Thread.sleep(10000); // Adjust the interval as needed (in milliseconds)
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String createQueue(String queueName) {
        try {
            CreateQueueResponse response = sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build());
            return response.queueUrl();
        } catch (SqsException e) {
            System.err.println("Error creating queue: " + e.getMessage());
            return null;
        }
    }

    public void sendMessageToQueue(String queueUrl, String messageBody) {
        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build());
        } catch (SqsException e) {
            System.err.println("Error sending message: " + e.getMessage());
        }
    }

    public ReceiveMessageResponse receiveMessageFromQueue(String queueUrl) {
        try {
            ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build());
            if (!response.messages().isEmpty()) {
                return response;
            }
        } catch (SqsException e) {
            System.err.println("Error receiving message: " + e.getMessage());
        }
        return null;
    }

    public void deleteMessageFromQueue(String queueUrl, String receiptHandle) {
        try {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            sqs.deleteMessage(deleteRequest);
        } catch (SqsException e) {
            System.err.println("Error deleting message: " + e.getMessage());
        }
    }

    public void deleteSqs(String sqsUrl){
        sqs.deleteQueue(DeleteQueueRequest.builder()
                                        .queueUrl(sqsUrl)
                                        .build());
    }

    //////////////////////////////// EC2 Methods ////////////////////////////////

    public String launchEC2Instance(String tagName, int max) {
        String instanceId = "";
        String script = WorkerScript;
        if (tagName.equals("Manager")) {
            script = ManagerScript;
        }

        try {
            RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(InstanceType.M4_LARGE)
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                    .keyName("vockey")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                    .build();
            RunInstancesResponse response = ec2.runInstances(runRequest);
            instanceId = response.instances().get(0).instanceId();
            System.out.println("EC2 instance launched: " + instanceId);

        } catch (Ec2Exception e) {
            System.err.println("Error launching EC2 instance: " + e.getMessage());
            return null;
        }
        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;

    }

    public void terminateEC2Instance(String instanceId) {
        try {
            ec2.terminateInstances(TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build());
            System.out.println("EC2 instance terminated: " + instanceId);
        } catch (Ec2Exception e) {
            System.err.println("Error terminating EC2 instance: " + e.getMessage());
        }
    }

    public List<String> receiveMessages(String SQS_URL, int i) {
        try {
            List<String> answer = new LinkedList<String>();
            for (int j = 1; j <= i; j++) {
                answer.add(receiveMessageFromQueue(SQS_URL).messages().get(0).body());

            }
            return answer;

        } catch (SqsException e) {
            System.err.println("Error receiving message: " + e.getMessage());
        }
        throw new java.lang.UnsupportedOperationException("Unimplemented method 'receiveMessages'");
    }

    public String getManagerInstanceId() {
        try {
            // Describe all instances
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse response = ec2.describeInstances(request);
    
            // Iterate over reservations and instances to find the "Manager"
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    boolean isManager = false;
                    for (Tag tag : instance.tags()) {
                        if (tag.key().equals("Name") && tag.value().equals("Manager")) {
                            isManager = true;
                            break;
                        }
                    }
    
                    // If instance has the "Manager" tag and is in RUNNING or PENDING state
                    if (isManager && (instance.state().name() == InstanceStateName.RUNNING 
                            || instance.state().name() == InstanceStateName.PENDING)) {
                        return instance.instanceId();
                    }
                }
            }
            System.out.println("No active Manager instance found.");
            return null; // Return null if no manager is found
    
        } catch (Ec2Exception e) {
            System.err.println("Error retrieving Manager instance ID: " + e.getMessage());
            return null;
        }
    }
    

}

