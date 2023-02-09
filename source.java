import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.*;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import com.github.shyiko.mysql.binlog.BinaryLogClient.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.sql.*;
import java.io.*;
import java.util.Scanner;
import java.net.*;

public class DCN {

    // CONFIGURATION VARS
    static final String USERNAME = "root";
    static final String PASSWORD = "";
    static final String DATABASE= "test";
    static final String HOSTNAME= "localhost";
    static final int PORT= 3307;
    static final String SOURCE="SOURCE1";
    static Map<String, String> tablesInfo =  new HashMap<String, String>(){{
        // TABLE NAME, PRIMARY_KEY
        put("user", "id");
    }};

    static final String MASTER_USERNAME = "root";
    static final String MASTER_PASSWORD = "password";
    static final String MASTER_DATABASE= "test";

    // MASTER PC HOSTNAME/IP
    // PUBLIC IP WHEN ON WAN
    // INTERNAL IP WHEN ON LAN
    // WHEN ON LAN, MAKE SURE TO SET STATIC INTERNAL IP FOR MASTER MAC ADDRESS IN ROUTER SETTINGS AND ADD PORT IN OS INBOUND TRAFFIC RULES
    static String MASTER_HOSTNAME= "192.168.10.3";

    // PORT OF DB SERVER ON MASTER
    static final int MASTER_PORT= 3306;

    // CONFIGURATION VARS


    static Map<String, List<String>> tablesColumns =  new HashMap<String, List<String>>();;
    
   
    static String tableName="";  
    public static void main(String[] args) throws Exception {
        BinaryLogClient client = new BinaryLogClient(HOSTNAME, PORT, USERNAME, PASSWORD);
        EventDeserializer eventDeserializer = new EventDeserializer();
        
        /*eventDeserializer.setCompatibilityMode(
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
            EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );*/

        try{
            initColumns();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        
        client.setEventDeserializer(eventDeserializer);

        
        client.registerEventListener(new EventListener() {

            @Override
            public void onEvent(Event event){
                
                EventType eventType=event.getHeader().getEventType();
                EventData data= event.getData();

                Connection masterConn=null;
                try{
                    masterConn=DriverManager.getConnection(
                        "jdbc:mysql://"+MASTER_HOSTNAME+":"+MASTER_PORT+"/"+MASTER_DATABASE+"?allowMultiQueries=true",
                        MASTER_USERNAME,
                        MASTER_PASSWORD
                    );
                }catch(Exception e){
                    //e.printStackTrace();
                    System.out.println("Master PC is down. Queries will be queued.");
                }

                String queuedQueries=getQueuedQueries();

                if(!queuedQueries.equals("") && masterConn!=null){
                    try{
                        Statement st = masterConn.createStatement();
                        int updateCount = st.executeUpdate(queuedQueries);
                        emptyQueue();
                    }catch(Exception e) {
                        e.printStackTrace();
                    }
                }

                
                if(data instanceof TableMapEventData) {
                    
                    TableMapEventData tableData = (TableMapEventData)data;
                    if(!tableData.getDatabase().equals(DATABASE))
                        return;
                    
                    String eventTableName=tableData.getTable();
                    
                    if(tablesInfo.keySet().contains(eventTableName))
                        tableName= eventTableName;
                    else
                        tableName= "";
                    
                }

                if(tableName.equals(""))
                    return;

                
                List<String> tableColumns= tablesColumns.get(tableName);
                String tablePK= tablesInfo.get(tableName);
                
                if(data instanceof UpdateRowsEventData) {
                    UpdateRowsEventData eventData = (UpdateRowsEventData)data;
                    
                    String updateQuery="";
                    for(Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {
                        updateQuery+="update "+ tableName+ " set ";
                        String updateCondition= "";
                        for(int i=0; i<tableColumns.size(); ++i){
                            updateQuery+= tableColumns.get(i) +"=\""+ row.getValue()[i]+"\",";

                            if(tableColumns.get(i).equals(tablePK))
                                updateCondition+= tablePK+"=\""+ row.getValue()[i] +"\"";
                        }
                        updateCondition+=" and source=\""+SOURCE+"\"";
                        updateQuery = updateQuery.substring(0, updateQuery.length() - 1);

                        updateQuery+=" where "+updateCondition+";";
                        
                    }

                    if(masterConn==null){
                        addToQueue(updateQuery);
                        return;
                    }
                    try{
                        Statement st = masterConn.createStatement();
                        int updateCount = st.executeUpdate(updateQuery);
                    }catch(Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(updateQuery);
                }

                if(data instanceof WriteRowsEventData) {
                    WriteRowsEventData eventData = (WriteRowsEventData)data;
                    String insertQuery="";

                    for(Object[] row: eventData.getRows()) {
                        insertQuery+="insert into "+ tableName+ "("+ String.join(",", tableColumns)+", source) values (";
                        for(int i=0; i<tableColumns.size(); ++i){
                            insertQuery+= "\""+ row[i]+"\",";
                        }
                        insertQuery+= "\""+SOURCE+"\");";
                        
                    }

                    if(masterConn==null){
                        addToQueue(insertQuery);
                        return;
                    }

                    try{
                        Statement st = masterConn.createStatement();
                        int updateCount= st.executeUpdate(insertQuery);
                    }catch(Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(insertQuery);
                }

                if(data instanceof DeleteRowsEventData) {
                    DeleteRowsEventData eventData = (DeleteRowsEventData)data;
                    String deleteQuery="";
                    for(Object[] row: eventData.getRows()) {
                        deleteQuery+="delete from "+tableName;
                        for(int i=0; i<tableColumns.size(); ++i){
                            if(tableColumns.get(i).equals(tablePK))
                                deleteQuery+= " where "+ tablePK+"=\""+row[i]+"\"";
                        }

                        deleteQuery+=" and source=\""+SOURCE+"\";";
                    }

                    if(masterConn==null){
                        addToQueue(deleteQuery);
                        return;
                    }

                    try{
                        Statement st = masterConn.createStatement();
                        int updateCount= st.executeUpdate(deleteQuery);
                    }catch(Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(deleteQuery);
                }
                try{
                    if(masterConn!=null)
                        masterConn.close();
                }catch(Exception e){

                }
            }
        });

        client.setUseSendAnnotateRowsEvent(true);
        try{
            client.connect();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }


    static Map<String, String> getValuesMap(Object[] product) {
        Map<String, String> map = new HashMap<>();
        map.put("id", java.lang.String.valueOf(product[0]));
        map.put("name", java.lang.String.valueOf(product[1]));
        map.put("update_timestamp", java.lang.String.valueOf(product[2]));

        return map;
    }

    public static void initColumns() throws Exception{
        
        Connection connection=DriverManager.getConnection(
            "jdbc:mysql://"+HOSTNAME+":"+PORT+"/"+DATABASE,
            USERNAME,
            PASSWORD
        );

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tableResultSet = metaData.getTables(null, "public", null, new String[]{"TABLE"});
        try {
            while (tableResultSet.next()) {
                String tableName = tableResultSet.getString("TABLE_NAME");

                if(!tablesInfo.keySet().contains(tableName))
                    continue;

                Statement st = connection.createStatement();
                ResultSet rset = st.executeQuery("SELECT * FROM "+tableName+" where 1=2");
                ResultSetMetaData md = rset.getMetaData();
                if(tablesColumns.get(tableName)==null)
                    tablesColumns.put(tableName, new ArrayList<String>());
                for (int i=1; i<=md.getColumnCount(); i++)
                {
                    tablesColumns.get(tableName).add(md.getColumnLabel(i));
                }
            }
        } finally {
            tableResultSet.close();
        }

        connection.close();
    }

    public static void addToQueue(String query){
        File queriesFile = new File("queuedQueries.txt");
        if (!queriesFile.exists()) {
            try{
                queriesFile.createNewFile();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        FileWriter fw = null;
        try
        {
            fw = new FileWriter("queuedQueries.txt",true); //the true will append the new data
            fw.write(query);//appends the string to the file
        }
        catch(IOException ioe)
        {
            System.err.println("IOException: " + ioe.getMessage());
        }
        finally{
            if(fw!=null)
                try{
                    fw.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
        }

    };

    public static String getQueuedQueries(){
        String queriesString="";
        try {
            File queriesFile = new File("queuedQueries.txt");
            Scanner queriesReader = new Scanner(queriesFile);
            while (queriesReader.hasNextLine()) {
                queriesString += queriesReader.nextLine();
            }
            queriesReader.close();
        
        } catch (FileNotFoundException e) {
            System.out.println("Queries File not Found.");
        }

        return queriesString;

    };
    
    public static void emptyQueue(){
        try{
            File queriesFile = new File("queuedQueries.txt");
            queriesFile.delete();
            queriesFile.createNewFile();
        } catch (Exception e) {
            System.out.println("Error during emptying queue.");
        }
    }
}