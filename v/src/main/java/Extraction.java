import java.io.*;

import com.google.protobuf.ServiceException;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class Extraction {

    //TODO implement logging, orgUnıtId must be Name;
    public static void main(String[] args) throws IOException, ServiceException, Exception {

        File configFile = new File("src/main/resources/config.properties");
        FileReader reader = new FileReader(configFile);

        Properties props = new Properties();
        props.load(reader);

        String tableName = props.getProperty("TableName");
        String family1 = props.getProperty("family1");
        String family2 = props.getProperty("family2");


        Test test = new Test();

        ArrayList<String> progID_list;
        ArrayList<Object> prog_list;

        test.createTable(tableName, family1, family2);

        /*
        prog_list holds the program objects, progID_list contains ID's of respective programs
        progID_list.get(1) == prog_list.get(1).[PARSE_ID]
         */
        progID_list = new ArrayList<String>();
        prog_list = test.getPrograms(progID_list);

        test.loadEvents(tableName, family1, family2, progID_list, prog_list);

        test.listContents(tableName, family1, family2);

        test.listTable();

    }

    /*
    Returns the arraylist of programs( as an object list)
    The string list given as a parameter contains the ID's of the programs
     */
    public ArrayList<Object> getPrograms(ArrayList<String> progIDList){
        ArrayList<Object> list = new ArrayList<Object>();
        try{
            File configFile = new File("src/main/resources/config.properties");
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            String base = props.getProperty("URL");
            String program = props.getProperty("program");
            String paging = props.getProperty("paging");
            String user = props.getProperty("user");
            String pass = props.getProperty("pwd");

            reader.close();

            URL url;
            url = new URL (base+program+paging);

            String encoding = new String(Base64.encodeBase64((user+":"+pass).getBytes()));

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + encoding);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            connection.connect();

            String jsonString = null;
            InputStream content = (InputStream)connection.getInputStream();
            InputStream content2 = (InputStream)connection.getInputStream();

            BufferedReader in = new BufferedReader (new InputStreamReader (content));
            list = readPrograms(in, progIDList);
            System.out.println("Size of progID " + progIDList.size());
            connection.disconnect();

        }
        catch (Exception e){
            e.printStackTrace();
        }
        return list;
    }

    /*
    Loads events by using the readEvents method
    //TODO Get the programids here
     */
    public void loadEvents(String table, String family1, String family2, ArrayList<String> ID_list, ArrayList<Object> metadata_list){

        String metaID;
        Object meta;
        ArrayList<Object> eventList;

        for(int i = 0; i< ID_list.size(); i++){
            metaID = ID_list.get(i);
            meta = metadata_list.get(i);
            eventList = new ArrayList<Object>();
            try{
                File configFile = new File("src/main/resources/config.properties");
                FileReader reader = new FileReader(configFile);

                Properties props = new Properties();
                props.load(reader);

                String base = props.getProperty("URL");
                String events = props.getProperty("events");
                String paging = props.getProperty("paging");
                String user = props.getProperty("user");
                String pass = props.getProperty("pwd");


                reader.close();

                URL url;
                url = new URL (base+events+"program="+metaID+paging);

                String encoding = new String(Base64.encodeBase64((user+":"+pass).getBytes()));

                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.setRequestProperty("Authorization", "Basic " + encoding);
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

                connection.connect();

                String jsonString = null;
                InputStream content = (InputStream)connection.getInputStream();
                BufferedReader in =  new BufferedReader (new InputStreamReader (content));

                readEvents(in,eventList);

                connection.disconnect();
                addToTable(table, family1, family2,"Individual", "program", meta ,eventList);
                System.out.println("Objects added for metadata id " + metaID );
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }


    }

    /*
    Returns the metadata programs as a whole object
     */
    private ArrayList<Object> readPrograms(BufferedReader reader, ArrayList<String> strList) throws IOException {
        String jsonString = null;
        String line;
        ArrayList<Object> jsonList = new ArrayList<Object>();

        while ((line = reader.readLine()) != null) {
            if (jsonString == null) jsonString = line;
            else jsonString = jsonString + line;
        }

        JSONObject obj = new JSONObject(jsonString);
        JSONArray elements = obj.getJSONArray("programs");
        Iterator<Object> iterator = elements.iterator();

        while (iterator.hasNext()){
            JSONObject flag = (JSONObject) iterator.next();
            String id = (flag.has("id")) ? (String)flag.get("id"):"NONE";
            strList.add(id);
            jsonList.add(flag);
        }
        System.out.println("Programs scanned.");
        return jsonList;
    }

    private void readEvents(BufferedReader reader, ArrayList<Object> events) throws IOException {
        String jsonString = null;
        String line;
        ArrayList<Object> jsonList = new ArrayList<Object>();

        while ((line = reader.readLine()) != null) {
            if (jsonString == null) jsonString = line;
            else jsonString = jsonString + line;
        }

        JSONObject obj = new JSONObject(jsonString);
        JSONArray elements = obj.getJSONArray("events");
        Iterator<Object> iterator = elements.iterator();

        while (iterator.hasNext()){
            JSONObject flag = (JSONObject) iterator.next();
            //String event_id = (String)flag.get("event");
            //System.out.println(event_id);
            jsonList.add(flag);
        }
        events.addAll(jsonList);

    }

    //TODO - DONE - if table exist already, dont create the table
    private void createTable(String tableName, String family1, String family2) throws IOException, ServiceException {

        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);

        //String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        //config.addResource(new Path(path));

        if (admin.tableExists(tableName)){
            System.out.println("Table already exists");
            return;
        }


        HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tableName));
        HColumnDescriptor data_family = new HColumnDescriptor(Bytes.toBytes(family1));
        HColumnDescriptor meta_family = new HColumnDescriptor(Bytes.toBytes(family2));

        table.addFamily(data_family);
        table.addFamily(meta_family);

        admin.createTable(table);

        System.out.println(tableName +" created.");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
            return;
        }

    }

    //TODO Exception: KeyValue size too large
    /*
    String type = ("Individual" or "Aggregated")
    String progOrdatasetVal = ("program" or "datasetvalue")
     */
    private void addToTable( String tableName, String family1, String family2,String type, String progOrdatasetval,Object meta, ArrayList<Object> dataList) throws IOException, ServiceException{

        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        BufferedMutator buffer = connection.getBufferedMutator(TableName.valueOf(tableName));

        //TODO  - DONE - time should be ms
        Put p = new Put( Bytes.toBytes("WIDP$"+type+"$"+System.currentTimeMillis()));

        p.addColumn(Bytes.toBytes(family1), Bytes.toBytes(family1), Bytes.toBytes((dataList.toString())));
        p.addColumn(Bytes.toBytes(family2), Bytes.toBytes(family2), Bytes.toBytes((meta.toString())));

        buffer.mutate(p);
    }

    private void listTable() throws IOException{
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);

        HTableDescriptor[] desc = admin.listTables();
        for( int i = 0; i < desc.length; i++) {
            System.out.println("Table: "+desc[i].getNameAsString());
            HColumnDescriptor[] col = desc[i].getColumnFamilies();
            for( int y = 0; y < col.length; y++){
                System.out.println(col[y].getNameAsString());

            }


        }

    }

    private void deleteTable(String tableName) throws IOException{
        Configuration config = HBaseConfiguration.create();

        HTable table = new HTable(config, tableName);
        Delete delete = new Delete(Bytes.toBytes("row1"));
    }

    private void listContents(String tableName, String family1, String family2 ) throws IOException{
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(config);

        //Connection connection = ConnectionFactory.createConnection(config);

        HTable table = new HTable(config, tableName);
        System.out.println("Table to list: " +table.getName());

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family1), Bytes.toBytes(family1));
        scan.addColumn(Bytes.toBytes(family2), Bytes.toBytes(family2));

        ResultScanner scanner = table.getScanner(scan);

        for (Result result = scanner.next(); (result != null); result = scanner.next()) {
            for(KeyValue keyValue : result.list()) {
                System.out.println("Key: " + keyValue.getKeyString() + "\nValue : " + Bytes.toString(keyValue.getValue()));
            }
        }


    }

}