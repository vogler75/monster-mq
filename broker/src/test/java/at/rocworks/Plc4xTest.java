package at.rocworks;

import org.apache.plc4x.java.api.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;

public class Plc4xTest {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage:");
            System.out.println("  java -cp ... at.rocworks.Plc4xTest <connection-string> <address-to-poll>");
            System.out.println("  java -cp ... at.rocworks.Plc4xTest <connection-string> -browse <symbol-query>");
            System.exit(1);
        }
        String connectionString = args[0];
        boolean isBrowse = args[1].equals("-browse");
        String addressOrQuery = isBrowse ? (args.length > 2 ? args[2] : "") : args[1];
        
        PlcConnection tempConnection = null;
        int attempt = 1;
        while (true) {
            System.out.println("Connecting to " + connectionString + " (attempt " + attempt + ") ...");
            try {
                tempConnection = PlcDriverManager.getDefault().getConnectionManager().getConnection(connectionString);
                if (tempConnection != null && tempConnection.isConnected()) {
                    System.out.println("Connected successfully!");
                    break;
                } else {
                    System.err.println("Connection established but isConnected() is false.");
                }
            } catch (Exception e) {
                System.err.println("Connection attempt " + attempt + " failed: " + e.getMessage());
            }
            System.out.println("Retrying in 5 seconds...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            attempt++;
        }
        
        try (PlcConnection plcConnection = tempConnection) {
            
            if (isBrowse) {
                if (!plcConnection.getMetadata().isBrowseSupported()) {
                    System.err.println("Browse is not supported by this connection!");
                    return;
                }
                System.out.println("Browsing for query: '" + addressOrQuery + "' ...");
                org.apache.plc4x.java.api.messages.PlcBrowseRequest browseRequest = plcConnection.browseRequestBuilder()
                    .addQuery("symbols", addressOrQuery)
                    .build();
                org.apache.plc4x.java.api.messages.PlcBrowseResponse browseResponse = browseRequest.execute().get();
                for (String queryName : browseResponse.getQueryNames()) {
                    System.out.println("Query: " + queryName);
                    for (org.apache.plc4x.java.api.messages.PlcBrowseItem item : browseResponse.getValues(queryName)) {
                        System.out.println("  - Name: " + item.getName());
                        System.out.println("    Tag: " + item.getTag());
                        System.out.println("    Options: " + item.getOptions());
                    }
                }
                return;
            }

            if (!plcConnection.getMetadata().isReadSupported()) {
                System.err.println("This connection does not support reading!");
                return;
            }
            
            System.out.println("Preparing read request for address: " + addressOrQuery);
            PlcReadRequest readRequest = plcConnection.readRequestBuilder()
                .addTagAddress("value", addressOrQuery)
                .build();
                
            System.out.println("Executing read request...");
            java.util.concurrent.CompletableFuture<? extends PlcReadResponse> future = readRequest.execute();
            PlcReadResponse response = null;
            try {
                response = future.get();
            } catch (Exception e) {
                System.err.println("Error occurred during future.get():");
                e.printStackTrace();
                System.err.println("--- START REFLECTION INSPECTION OF FUTURE ---");
                logObjectFields(future, "future", 4, 0, new java.util.HashSet<>());
                System.err.println("--- END REFLECTION INSPECTION OF FUTURE ---");
                throw e;
            }
            
            System.out.println("Response status for 'value': " + response.getResponseCode("value"));
            if (response.getResponseCode("value") == PlcResponseCode.OK) {
                Object val = response.getObject("value");
                System.out.println("Parsed object class: " + (val != null ? val.getClass().getName() : "null"));
                System.out.println("Raw Value: " + val);
            } else {
                System.err.println("Read failed with response code: " + response.getResponseCode("value"));
            }
        } catch (Exception e) {
            System.err.println("Error occurred during PLC4X operation:");
            e.printStackTrace();
            System.err.println("--- START REFLECTION INSPECTION OF EXCEPTION ---");
            logObjectFields(e, "exception", 4, 0, new java.util.HashSet<>());
            System.err.println("--- END REFLECTION INSPECTION OF EXCEPTION ---");
        }
    }

    private static void logObjectFields(Object obj, String name, int maxDepth, int currentDepth, java.util.Set<Integer> visited) {
        if (obj == null) return;
        int objId = System.identityHashCode(obj);
        if (visited.contains(objId)) return;
        visited.add(objId);
        if (currentDepth > maxDepth) return;
        Class<?> clazz = obj.getClass();
        try {
            Class<?> currentClass = clazz;
            while (currentClass != null && currentClass != Object.class) {
                for (java.lang.reflect.Field field : currentClass.getDeclaredFields()) {
                    try {
                        field.setAccessible(true);
                        Object value = field.get(obj);
                        if (value != null) {
                            String valueStr;
                            if (value instanceof java.util.Map) {
                                java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
                                java.util.List<String> keyStrings = new java.util.ArrayList<>();
                                for (Object k : map.keySet()) {
                                    keyStrings.add(k != null ? k.toString() : "null");
                                }
                                valueStr = "Map(size=" + map.size() + ", keys=" + keyStrings + ")";
                            } else if (value instanceof java.util.Collection) {
                                java.util.Collection<?> col = (java.util.Collection<?>) value;
                                java.util.List<String> colStrings = new java.util.ArrayList<>();
                                for (Object item : col) {
                                    colStrings.add(item != null ? item.toString() : "null");
                                }
                                valueStr = "Collection(size=" + col.size() + ", elements=" + colStrings + ")";
                            } else {
                                valueStr = value.toString();
                            }
                            System.out.println("  ".repeat(currentDepth + 1) + "Field " + field.getName() + " (" + field.getType().getName() + ") = " + valueStr);
                            
                            String typeName = field.getType().getName();
                            if (currentDepth < maxDepth && 
                                !field.getType().isPrimitive() && 
                                !typeName.startsWith("java.lang") && 
                                !typeName.startsWith("java.util") && 
                                !value.getClass().isEnum()) {
                                logObjectFields(value, field.getName(), maxDepth, currentDepth + 1, visited);
                            }
                        }
                    } catch (Exception ex) {
                        // ignore
                    }
                }
                currentClass = currentClass.getSuperclass();
            }
        } catch (Exception ex) {
            // ignore
        }
    }
}
