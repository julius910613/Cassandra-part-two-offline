import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by fanchenli on 2/4/14.
 */
public class ClientLi {

    private Cluster cluster;

    private Session session;

    private  PreparedStatement rowCQL =null;



    public void createSchema(){
        //create keyspace

        cluster =  Cluster.builder().addContactPoint("ec2-54-194-196-168.eu-west-1.compute.amazonaws.com").build();


        final Session bootstartupSession = cluster.connect();
        String keyspaceCQL = "CREATE KEYSPACE LI_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}";

        bootstartupSession.execute(keyspaceCQL);
        bootstartupSession.shutdown();

        session = cluster.connect("li_keyspace");

       // session.execute(keyspaceCQL);
        System.out.print("\n user1 keyspace created");



        String tableCQL = "CREATE TABLE LI_keyspace.URLRecords (" +
                "Client_id int, " +
                "TimeStamp timestamp, " +
                "Action text, " +
                "Status text," +
                "Size text," +
                "PRIMARY KEY (Client_id, TimeStamp, Action)" +
                ");";
        session.execute(tableCQL);
        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
                "VALUES (? ,? ,? ,? ,?);");

        System.out.println("table created");

    }



    public void insertRow(int clientID, Date timestamp, String action, String status,String size){

        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(rowCQL).bind(clientID, timestamp, action,status,size ));
       // System.out.print("\n data has been inserted");
    }


    public void countActivity(String c_id, String S_time, String E_time){
        //String countCQL = "select url, client_id from client_table where client_id = " + " ' "+c_id + " ' " + "and timespot >=" + " ' " + S_time + " ' " + " and timespot <=' " + E_time + " ';";
        PreparedStatement countCQL = session.prepare("SELECT url, client_id FROM client_table WHERE client_id = ? and timespot >= ? and timespot <= ? ");
        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(c_id, S_time,E_time ));

        ResultSet rset = queryFuture.getUninterruptibly();
        for(Row row:rset){
           System.out.println(row.getString(0)+" " + row.getString(1));
        }



    }


    public void countNumOfURL(ArrayList<String> URLlist, String s_time, String e_time){
        PreparedStatement countCQL = session.prepare("SELECT count(*) FROM url_table WHERE url = ? and timespot >= ? and timespot <= ? ");
        for(int i = 0; i < URLlist.size(); i ++){
            String url = URLlist.get(i);
            //String countCQL = "select count(*) from url_table where url = ' " + url + " ' and timespot >=" + " ' " + s_time + " ' " + " and timespot <=' " + e_time + " ';";

            ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(URLlist.get(i), s_time,e_time ));

            ResultSet rset = queryFuture.getUninterruptibly();
            for(Row row:rset){
                System.out.println(url + " " + row.getLong(0));
            }
        }
    }

    public void connect(){
        cluster =  Cluster.builder().addContactPoint("ec2-54-194-196-168.eu-west-1.compute.amazonaws.com").build();

        session = cluster.connect("li_keyspace");

        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
                "VALUES (? ,? ,? ,? ,?);");
    }


    public void CountNumber(int clientID){
        boolean inti = true;
        HashMap<String, Integer> urlCountMap = new HashMap<String, Integer>();

        PreparedStatement searchCQL = session.prepare("select client_id,timestamp,action from urlrecords where client_id = ?");
        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchCQL).bind(clientID));
        ResultSet rset = queryFuture.getUninterruptibly();
        SiteSession ss = new SiteSession();
        for(Row row:rset){

            int cid = row.getInt(0);
            String id= cid + "";
            Date date = row.getDate(1);

            long firstHitMillis = date.getTime();
            String url = row.getString(2);
            if(inti == true){
               ss.setID(id);
                ss.setFirstHitMillis(firstHitMillis);
                //ss.update(firstHitMillis,url);
                ss.addHitCount(firstHitMillis,url);
                inti = false;
            }
            else{
                ss.update(firstHitMillis,url);
                if(ss.getflagNormal()){
                    ss.addHitCount(firstHitMillis,url);
                }
                if(ss.getTimeOut()){
                    System.out.println("Session expired! " + "Start time: " + ss.getFirstHitMillis() + " End time: " + ss.getLastHitMillis() + " number of access: " + ss.getHitCount() + " number of url access: " + ss.getHyperLogLog().cardinality());
                    ss.reset();

                    ss.setID(id);
                    ss.setFirstHitMillis(firstHitMillis);
                    //ss.update(firstHitMillis,url);
                    ss.addHitCount(firstHitMillis,url);
                }
            }

        }



    }


    private void close() {
        cluster.shutdown();
    }

    /*public static void main(String[] args){
        ArrayList<String> urlList = new ArrayList<String>();
        urlList.add("3");
        urlList.add("4");
        urlList.add("5");
        ClientLi cl = new ClientLi();
        cl.connect();
        cl.createSchema();
        //cl.countActivity("1", "0", "3");
        cl.countNumOfURL(urlList, "0", "3");
        cl.close();

    }*/




}
