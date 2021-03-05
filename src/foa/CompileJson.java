package foa;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import net.sf.jasperreports.engine.JRParameter;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.data.JsonQLDataSource;
import net.sf.jasperreports.engine.export.JRXlsExporter;
import net.sf.jasperreports.engine.export.ooxml.JRXlsxExporter;
import net.sf.jasperreports.engine.json.JRJsonNode;
import net.sf.jasperreports.engine.query.JsonQueryExecuterFactory;
import net.sf.jasperreports.engine.util.JRLoader;
import net.sf.jasperreports.export.Exporter;
import net.sf.jasperreports.export.SimpleExporterInput;
import net.sf.jasperreports.export.SimpleOutputStreamExporterOutput;
import net.sf.jasperreports.export.SimpleXlsReportConfiguration;
import net.sf.jasperreports.export.SimpleXlsxReportConfiguration;

public class CompileJson {
	
	
	public static void main(String[] args) throws Exception {
		startServer(args);
//		generarDesdeArchivo();
	}
	
	static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
        	
        	t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

//            if (t.getRequestMethod().equalsIgnoreCase("OPTIONS")) {
//                t.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
//                t.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Authorization");
//                t.sendResponseHeaders(204, -1);
//                return;
//            }

        	try {
        		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        		generar(t.getRequestBody(), baos, t);
        		t.sendResponseHeaders(200, baos.size());
        		OutputStream os = t.getResponseBody();
				baos.writeTo(os);
        		os.flush();
        		os.close();
			} catch (Exception e) {
				String response = e.getMessage();
	            t.sendResponseHeaders(500, response.length());
	            OutputStream os = t.getResponseBody();
	            os.write(response.getBytes());
	            os.close();
			}
            
        }
    }

	private static void startServer(String[] args) throws IOException {
		HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(args[0])), 0);
        server.createContext("/", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
	}
	
	public static void generar(InputStream is, OutputStream os, HttpExchange t) throws Exception {
//		JasperCompileManager.compileReportToFile("/home/blas/examples/jsonql.jrxml", "/home/blas/examples/jsonql.jasper");
		
		Locale locale = new Locale("pt", "BR");
		
		JsonQLDataSource datasource = new JsonQLDataSource(is, "data");
		
//		File jsonFile = new File("/home/blas/examples/accounts.json");
//		JsonQLDataSource datasource = new JsonQLDataSource(jsonFile, jsonqlExpression);
		
		Field privateStringField = datasource.getClass().getDeclaredField("root");
		privateStringField.setAccessible(true);
		JRJsonNode root = (JRJsonNode) privateStringField.get(datasource);
		
		// Jasper File to fill and Output type
		String jasperFile = root.getDataNode().get("jasperFile").asText();
		if (!Files.exists(Paths.get(jasperFile))) {
			throw new Exception("Jasper File does not exists.");
		}
		String outputType = root.getDataNode().get("outputType").asText();
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> params = mapper.convertValue(root.getDataNode().get("filters"), new TypeReference<Map<String, Object>>(){});
		params.put(JsonQueryExecuterFactory.JSON_DATE_PATTERN, "yyyy-MM-dd");
		params.put(JsonQueryExecuterFactory.JSON_NUMBER_PATTERN, "#,##0.##");
		params.put(JsonQueryExecuterFactory.JSON_LOCALE, locale);
		params.put(JRParameter.REPORT_LOCALE, locale);
		
		datasource.setLocale(locale);
		
		JasperReport jr = (JasperReport) JRLoader.loadObjectFromFile(jasperFile);
		JasperPrint jp = JasperFillManager.fillReport(jr, params, datasource);
		if ("pdf".equals(outputType)) {
			t.getResponseHeaders().add("Content-Type", "application/pdf");
    		t.getResponseHeaders().add("Content-disposition", "inline; filename=report.pdf");
			JasperExportManager.exportReportToPdfStream(jp, os);
//			JasperExportManager.exportReportToPdfFile(jp, "/home/blas/examples/accounts.pdf");
		} else if ("xls".equals(outputType)) {
			t.getResponseHeaders().add("Content-Type", "application/xls");
    		t.getResponseHeaders().add("Content-disposition", "inline; filename=report.xls");
			JRXlsExporter xlsExporter = new JRXlsExporter();
            xlsExporter.setExporterInput(new SimpleExporterInput(jp));
            xlsExporter.setExporterOutput(new SimpleOutputStreamExporterOutput(os));
            SimpleXlsReportConfiguration xlsReportConfiguration = new SimpleXlsReportConfiguration();
            xlsReportConfiguration.setOnePagePerSheet(true);
            xlsReportConfiguration.setRemoveEmptySpaceBetweenRows(true);
            xlsReportConfiguration.setDetectCellType(true);
            xlsReportConfiguration.setWhitePageBackground(false);
            xlsExporter.setConfiguration(xlsReportConfiguration);
            xlsExporter.exportReport();
		} else if ("xlsx".equals(outputType)) {
			t.getResponseHeaders().add("Content-Type", "application/xlsx");
    		t.getResponseHeaders().add("Content-disposition", "inline; filename=report.xlsx");
			SimpleXlsxReportConfiguration configuration = new SimpleXlsxReportConfiguration();
			configuration.setOnePagePerSheet(false);
			configuration.setIgnoreGraphics(false);

			try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			    Exporter exporter = new JRXlsxExporter();
			    exporter.setExporterInput(new SimpleExporterInput(jp));
			    exporter.setExporterOutput(new SimpleOutputStreamExporterOutput(byteArrayOutputStream));
			    exporter.setConfiguration(configuration);
			    exporter.exportReport();
			    byteArrayOutputStream.writeTo(os);
			}
		}
	}
	
	public static void generarDesdeArchivo() throws Exception {
		
		String outputType = "pdf";
		String jsonFile = "/home/blas/Downloads/libro_ventas/libro_ventas.json";
		String jasperFile = "/home/blas/Downloads/libro_ventas/Libro_Ventas_Quiebre_por_Porcentaje_de_IVA.jasper";
		String outputFile = "/home/blas/Downloads/libro_ventas/libro_ventas."+outputType;
		
		Locale locale = new Locale("pt", "BR");
		
		JsonQLDataSource datasource = new JsonQLDataSource(new File(jsonFile), "message.result");
		
		Field privateStringField = datasource.getClass().getDeclaredField("root");
		privateStringField.setAccessible(true);
//		JRJsonNode root = (JRJsonNode) privateStringField.get(datasource);
		
		// Jasper File to fill and Output type
		
		if (!Files.exists(Paths.get(jasperFile))) {
			throw new Exception("Jasper File does not exists.");
		}
		
		
//		ObjectMapper mapper = new ObjectMapper();
//		Map<String, Object> params = mapper.convertValue(root.getDataNode().get("filters"), new TypeReference<Map<String, Object>>(){});
		Map<String, Object> params = new HashMap<>();
		params.put(JsonQueryExecuterFactory.JSON_DATE_PATTERN, "yyyy-MM-dd");
		params.put(JsonQueryExecuterFactory.JSON_NUMBER_PATTERN, "#,##0.##");
		params.put(JsonQueryExecuterFactory.JSON_LOCALE, locale);
		params.put(JRParameter.REPORT_LOCALE, locale);
		
		datasource.setLocale(locale);
		
		JasperReport jr = (JasperReport) JRLoader.loadObjectFromFile(jasperFile);
		JasperPrint jp = JasperFillManager.fillReport(jr, params, datasource);
		if ("pdf".equals(outputType)) {
//			JasperExportManager.exportReportToPdfStream(jp, os);
			JasperExportManager.exportReportToPdfFile(jp, "/home/blas/Downloads/libro_ventas/libro_ventas.pdf");
		} else if ("xls".equals(outputType)) {
			JRXlsExporter xlsExporter = new JRXlsExporter();
            xlsExporter.setExporterInput(new SimpleExporterInput(jp));
            xlsExporter.setExporterOutput(new SimpleOutputStreamExporterOutput(new File(outputFile)));
            SimpleXlsReportConfiguration xlsReportConfiguration = new SimpleXlsReportConfiguration();
            xlsReportConfiguration.setOnePagePerSheet(true);
            xlsReportConfiguration.setRemoveEmptySpaceBetweenRows(true);
            xlsReportConfiguration.setDetectCellType(true);
            xlsReportConfiguration.setWhitePageBackground(false);
            xlsExporter.setConfiguration(xlsReportConfiguration);
            xlsExporter.exportReport();
		} else if ("xlsx".equals(outputType)) {
			SimpleXlsxReportConfiguration configuration = new SimpleXlsxReportConfiguration();
			configuration.setOnePagePerSheet(false);
			configuration.setIgnoreGraphics(false);

			try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(); FileOutputStream fos = new FileOutputStream(new File(outputFile))) {
			    Exporter exporter = new JRXlsxExporter();
			    exporter.setExporterInput(new SimpleExporterInput(jp));
			    exporter.setExporterOutput(new SimpleOutputStreamExporterOutput(byteArrayOutputStream));
			    exporter.setConfiguration(configuration);
			    exporter.exportReport();
			    byteArrayOutputStream.writeTo(fos);
			}
		}
	}
	
}
