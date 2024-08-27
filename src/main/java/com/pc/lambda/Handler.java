package com.pc.lambda;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Handler implements RequestHandler<SQSEvent, String> {

	private static Gson gson = null;
	private static LambdaLogger logger = null;

	public String handleRequest(SQSEvent event, Context context) {
		
		logger = context.getLogger();
		gson = new GsonBuilder().setPrettyPrinting().create();

		logger.log("\nEvent => " + gson.toJson(event));

		String input = null;
		for (SQSMessage msg : event.getRecords()) {
			input = msg.getBody();
			logger.log("\nSQS Message: " + input);

			// Read all of the environment variables
			Boolean readFile = Boolean.valueOf(System.getenv("readFile"));
			Boolean writeFile = Boolean.valueOf(System.getenv("writeFile"));
			Boolean writeReadLogs = Boolean.valueOf(System.getenv("writeReadLogs"));
			String fileDirectory = System.getenv("efsFolder");
			String fileName = System.getenv("fileName");

			// Prepare the file name to be processed
			String fullFilePath = fileDirectory + fileName;

			logger.log("\nreadFile => " + readFile);
			logger.log("\nwriteFile => " + writeFile);
			logger.log("\nwriteReadLogs => " + writeReadLogs);
			logger.log("\nfileDirectory => " + fileDirectory);
			logger.log("\nfileName => " + fileName);
			logger.log("\nfullFilePath => " + fullFilePath);
			logger.log("\ncontext => " + gson.toJson(context));

			// Create the file directory if it doesn't exist
			Path dir = Paths.get(fileDirectory);
			if (Files.exists(dir)) {
				logger.log("\n!! Directory Available !!");
			} else {
				logger.log("\n!! Directory Unavailable !!");
				try {
					logger.log("\n!! Creating Directory !!");
					Files.createDirectories(dir);
					logger.log("\n!! Created Directory !!");
				} catch (IOException e) {
					logger.log("\n!! Error while creating directory !!");
					return "\\n!! Error while creating directory !!";
				}
			}

			// Verify whether the file to be processed exists or not
			Path newFilePath = Paths.get(fullFilePath);
			Boolean fileExists = Files.exists(newFilePath);
			if (fileExists) {
				logger.log("\nFile is available: " + newFilePath.toAbsolutePath().toString());
			} else {
				logger.log("\nFile is unavailable: " + newFilePath.toAbsolutePath().toString());
			}

			try {
				// Write File
				if (writeFile) {
					writeExcelFile(fullFilePath);
					fileExists = true;
					logger.log("\nFile written successfully: " + fullFilePath);
				}

				// Read File
				if (fileExists && readFile) {
					readExcelFile(fullFilePath, writeReadLogs);
					logger.log("\nFile read successfully: " + fullFilePath);
				}
			} catch (Exception e) {
				logger.log("\nException while performing file operations: " + e.getMessage() + " \nStack Trace: "
						+ e.getStackTrace());
			}

			logger.log("\n\n");
		}

		return "Processed successfully";
	}

	private void writeExcelFile(String filePath) throws IOException {

		logger.log("\nInside writeExcelFile method.");

		// Blank workbook
		try (XSSFWorkbook workbook = new XSSFWorkbook()) {

			logger.log("\nInitialized an instance of XSSFWorkbook");

			// Create a blank sheet
			XSSFSheet sheet = workbook.createSheet("Sample Data");

			logger.log("\nCreated blank sheet.");

			// This data needs to be written (Object[])
			Map<String, Object[]> data = new TreeMap<String, Object[]>();
			data.put("1", new Object[] { "ID", "NAME", "LASTNAME" });
			data.put("2", new Object[] { 1, "Amit", "Shukla" });
			data.put("3", new Object[] { 2, "Lokesh", "Gupta" });
			data.put("4", new Object[] { 3, "John", "Adwards" });
			data.put("5", new Object[] { 4, "Brian", "Schultz" });

			// Iterate over data and write to sheet
			Set<String> keyset = data.keySet();
			int rownum = 0;
			for (String key : keyset) {
				Row row = sheet.createRow(rownum++);
				Object[] objArr = data.get(key);
				int cellnum = 0;
				for (Object obj : objArr) {
					Cell cell = row.createCell(cellnum++);
					if (obj instanceof String)
						cell.setCellValue((String) obj);
					else if (obj instanceof Integer)
						cell.setCellValue((Integer) obj);
				}
			}

			logger.log("\nPopulated data.");

			// Write the workbook in file system
			try (FileOutputStream out = new FileOutputStream(new File(filePath))) {
				logger.log("\nInitialized an instance of FileOutputStream.");
				workbook.write(out);
			}

			logger.log("\nwriteExcelFile method executed.");
		}
	}

	private void readExcelFile(String filePath, Boolean writeReadLogs) throws FileNotFoundException, IOException {
		try (FileInputStream file = new FileInputStream(new File(filePath))) {

			logger.log("\nInside readExcelFile method.");

			// Create Workbook instance holding reference to .xlsx file
			try (XSSFWorkbook workbook = new XSSFWorkbook(file)) {
				for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); ++sheetIndex) {
					// Get the desired sheet from the workbook
					XSSFSheet sheet = workbook.getSheetAt(sheetIndex);

					logger.log("\nSheet Name: " + sheet.getSheetName());
					logger.log("\nRow Count: " + sheet.getPhysicalNumberOfRows());

					// Iterate through each rows one by one
					Iterator<Row> rowIterator = sheet.iterator();
					while (rowIterator.hasNext()) {
						Row row = rowIterator.next();
						// For each row, iterate through all the columns
						Iterator<Cell> cellIterator = row.cellIterator();

						while (cellIterator.hasNext()) {
							Cell cell = cellIterator.next();
							// Check the cell type and format accordingly
							switch (cell.getCellType()) {
							case Cell.CELL_TYPE_NUMERIC:
								if (writeReadLogs) {
									logger.log(cell.getNumericCellValue() + "\t");
								}
								break;
							default:
								if (writeReadLogs) {
									logger.log(cell.getStringCellValue() + "\t");
								}
								break;
							}
						}
					}
				}
			}

			logger.log("\nreadExcelFile method executed.");
		}
	}

	/*
	 * private void executeStepFunction() { try { StartExecutionRequest request =
	 * new StartExecutionRequest()
	 * .withStateMachineArn(System.getenv("stepFunctionARN")).withInput(input);
	 * 
	 * AWSStepFunctions sfnClient =
	 * AWSStepFunctionsClientBuilder.standard().withRegion(Regions.US_EAST_1).build(
	 * ); StartExecutionResult result = sfnClient.startExecution(request);
	 * logger.log("\nStep Function Request ID: " +
	 * result.getSdkResponseMetadata().getRequestId()); } catch
	 * (AmazonServiceException ase) {
	 * logger.log("Caught an AmazonServiceException, which means" +
	 * " your request made it to Amazon Step Functions, but was" +
	 * " rejected with an error response for some reason.");
	 * logger.log("Error Message:    " + ase.getMessage());
	 * logger.log("HTTP Status Code: " + ase.getStatusCode());
	 * logger.log("AWS Error Code:   " + ase.getErrorCode());
	 * logger.log("Error Type:       " + ase.getErrorType());
	 * logger.log("Request ID:       " + ase.getRequestId()); } catch
	 * (AmazonClientException ace) {
	 * logger.log("Caught an AmazonClientException, which means " +
	 * "the client encountered a serious internal problem while " +
	 * "trying to communicate with Step Functions, such as not " +
	 * "being able to access the network."); logger.log("Error Message: " +
	 * ace.getMessage()); } }
	 */
}
