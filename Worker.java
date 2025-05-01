package may;

//import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.File;
import java.io.IOException;
//import java.util.List;

public class Worker {

    private static String MANAGER_TO_WORKER ;
    private static String WORKER_TO_MANAGER;
    private static String LOCAL_ID;
    private final static AWS aws = AWS.getInstance();

    private static void processTask(ReceiveMessageResponse task) throws IOException {
        String messageBody = task.messages().get(0).body();
        String[] parts = messageBody.split("\t");
        if (parts.length != 3) {
            System.err.println("Invalid task format: " + task);
            for(int i=0;i<parts.length;i++){
                System.out.println(parts[i]);
            }
            return;
        }
        String command = parts[0];
        String pdfUrl = parts[1];
        LOCAL_ID = parts[2];

        File output=PDFProcessor.processPDF(command, pdfUrl);
        if(output==null){
            System.err.println("couldnt create file" + task);
            aws.sendMessageToQueue(WORKER_TO_MANAGER,pdfUrl+ "\t"+ "failed: "+"\t"+"couldnt create file"+"\t"+LOCAL_ID );       
            return; 
        }
        String newFileUrl= aws.uploadFileToS3(LOCAL_ID, pdfUrl, output);
        System.out.println("worker uploaded processed file.");
        aws.sendMessageToQueue(WORKER_TO_MANAGER, "Processed: " + pdfUrl+ "\t" +newFileUrl+"\t"+command+"\t"+LOCAL_ID);
    }


    public static void main(String[] args) {
        System.err.println("worker started");
        
        MANAGER_TO_WORKER = aws.getQueueUrl("MANAGER_TO_WORKER");
        WORKER_TO_MANAGER = aws.getQueueUrl("WORKER_TO_MANAGER");

        while (true) {
            ReceiveMessageResponse To_Do = aws.receiveMessageFromQueue(MANAGER_TO_WORKER);
            if (To_Do != null) {
                try {
                    aws.deleteMessageFromQueue(MANAGER_TO_WORKER, To_Do.messages().get(0).receiptHandle());
                    processTask(To_Do);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                try{
                    Thread.sleep(5000);
                } catch(Exception e){
                    System.out.println("worker cant sleep.");
                }

            }

        }

    }

}