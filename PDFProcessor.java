package may;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

public class PDFProcessor {

    /**
     * Processes a single PDF URL with the given command.
     * 
     * @param command The command: ToImage, ToHTML, or ToText.
     * @param pdfUrl  The URL of the PDF to process.
     * @throws IOException If an error occurs while reading or processing the PDF.
     */
    public static File processPDF(String command, String pdfUrl) throws IOException {       
        try {
            File tempFile = downloadPDF(pdfUrl);
            PDDocument document = PDDocument.load(tempFile); 
            if (document.isEncrypted()) {
                System.out.println("Cannot process encrypted PDF: " + pdfUrl);
                return null;
            }

            // Process the first page of the PDF
            switch (command) {
                case "ToImage":
                    return convertToImage(document, pdfUrl);

                case "ToHTML":
                    return convertToHTML(document, pdfUrl);

                case "ToText":
                    return convertToText(document, pdfUrl);

                default:
                    System.out.println("Unknown command: " + command);
                    return null;
            }
        }  catch (Exception e) {
            System.err.println("bad URL");
            e.printStackTrace();
            return null;
        }finally {
        }
    }

    /**
     * Downloads a PDF from a URL and saves it to a temporary file.
     * 
     * @param pdfUrl The URL of the PDF to download.
     * @return A temporary File containing the downloaded PDF.
     * @throws IOException If an error occurs while downloading the file.
     */
    private static File downloadPDF(String pdfUrl) throws IOException {
        File tempFile = File.createTempFile("pdf", ".pdf");
        URLConnection connection = new URL(pdfUrl).openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        try (InputStream in = new URL(pdfUrl).openStream();
                FileOutputStream out = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return tempFile;
    }

    /**
     * Converts the first page of the PDF to an image (PNG).
     * 
     * @param document The loaded PDDocument.
     * @param pdfUrl   The original URL of the PDF.
     * @return
     * @throws IOException If an error occurs during conversion.
     */
    private static File convertToImage(PDDocument document, String pdfUrl) throws IOException {
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0); // Render the first page (index 0)

        // Save the image
        File outputFile = new File("output/" + sanitizeFilename(pdfUrl) + ".png");
        ImageIO.write(image, "PNG", outputFile);
        // System.out.println("Saved image: " + outputFile.getAbsolutePath());
        return outputFile;
    }

    /**
     * Converts the first page of the PDF to HTML.
     * 
     * @param document The loaded PDDocument.
     * @param pdfUrl   The original URL of the PDF.
     * @throws IOException If an error occurs during conversion.
     */
    private static File convertToHTML(PDDocument document, String pdfUrl) throws IOException {
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(1);
        stripper.setEndPage(1);
        String text = stripper.getText(document);

        // Convert to basic HTML (you can enhance this formatting)
        String htmlContent = "<html><body><pre>" + text + "</pre></body></html>";

        // Save the HTML
        File outputFile = new File("output/" + sanitizeFilename(pdfUrl) + ".html");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(htmlContent);
        }
        return outputFile;
    }

    /**
     * Converts the first page of the PDF to plain text.
     * 
     * @param document The loaded PDDocument.
     * @param pdfUrl   The original URL of the PDF.
     * @throws IOException If an error occurs during conversion.
     */
    private static File convertToText(PDDocument document, String pdfUrl) throws IOException {
        PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(1);
        stripper.setEndPage(1);
        String text = stripper.getText(document);

        // Save the text
        File outputFile = new File("output/" + sanitizeFilename(pdfUrl) + ".txt");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(text);
        }
        return outputFile;
    }

    /**
     * Sanitizes a filename by removing illegal characters.
     * 
     * @param url The URL to sanitize.
     * @return A sanitized filename.
     */
    private static String sanitizeFilename(String url) {
        return url.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
    }
}
