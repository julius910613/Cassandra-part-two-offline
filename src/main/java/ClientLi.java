import com.datastax.driver.core.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by fanchenli on 2/4/14.
 */
public class ClientLi {

    private Cluster cluster;

    private Session session;

    private PreparedStatement rowCQL = null;

    private PreparedStatement searchCQL = null;

    private PreparedStatement searchSessionByIDCQL = null;

    private PreparedStatement insertSessionCQL = null;


//    public void createSchema() {
//        //create keyspace
//
//        cluster = Cluster.builder().addContactPoint("localhost").build();
//
//
//        final Session bootstartupSession = cluster.connect();
//        String keyspaceCQL = "CREATE KEYSPACE LI_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}";
//
//        bootstartupSession.execute(keyspaceCQL);
//        bootstartupSession.shutdown();
//
//        session = cluster.connect("li_keyspace");
//
//        // session.execute(keyspaceCQL);
//        System.out.print("\n user1 keyspace created");
//
//
//        String tableCQL = "CREATE TABLE LI_keyspace.URLRecords (" +
//                "Client_id int, " +
//                "TimeStamp timestamp, " +
//                "Action text, " +
//                "Status text," +
//                "Size text," +
//                "PRIMARY KEY (Client_id, TimeStamp, Action)" +
//                ");";
//        session.execute(tableCQL);
//        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
//                "VALUES (? ,? ,? ,? ,?);");
//
//        System.out.println("table created");
//
//    }


    public void insertRow(int clientID, Date timestamp, String action, String status, String size) {

        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
                "VALUES (? ,? ,? ,? ,?);");

        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(rowCQL).bind(clientID, timestamp, action, status, size));
        // System.out.print("\n data has been inserted");
    }


//    public void countActivity(String c_id, String S_time, String E_time) {
//        //String countCQL = "select url, client_id from client_table where client_id = " + " ' "+c_id + " ' " + "and timespot >=" + " ' " + S_time + " ' " + " and timespot <=' " + E_time + " ';";
//        PreparedStatement countCQL = session.prepare("SELECT url, client_id FROM client_table WHERE client_id = ? and timespot >= ? and timespot <= ? ");
//        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(c_id, S_time, E_time));
//
//        ResultSet rset = queryFuture.getUninterruptibly();
//        for (Row row : rset) {
//            System.out.println(row.getString(0) + " " + row.getString(1));
//        }
//
//
//    }


//    public void countNumOfURL(ArrayList<String> URLlist, String s_time, String e_time) {
//        PreparedStatement countCQL = session.prepare("SELECT count(*) FROM url_table WHERE url = ? and timespot >= ? and timespot <= ? ");
//        for (int i = 0; i < URLlist.size(); i++) {
//            String url = URLlist.get(i);
//            //String countCQL = "select count(*) from url_table where url = ' " + url + " ' and timespot >=" + " ' " + s_time + " ' " + " and timespot <=' " + e_time + " ';";
//
//            ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(URLlist.get(i), s_time, e_time));
//
//            ResultSet rset = queryFuture.getUninterruptibly();
//            for (Row row : rset) {
//                System.out.println(url + " " + row.getLong(0));
//            }
//        }
//    }

    public void connect() {
        cluster = Cluster.builder().addContactPoint("localhost").build();

        session = cluster.connect("li_keyspace");

    }

    public void CreateSessionTable() {
        cluster = Cluster.builder().addContactPoint("localhost").build();


        final Session bootstartupSession = cluster.connect();
        String keyspaceCQL = "CREATE KEYSPACE LI_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}";

        bootstartupSession.execute(keyspaceCQL);
        bootstartupSession.shutdown();

        session = cluster.connect("li_keyspace");

        String tableCQL = "CREATE TABLE LI_keyspace.SessionRecords (" +
                "Client_id int, " +
                "StartTime timestamp, " +
                "EndTime timestamp, " +
                "numberOfAccess int," +
                "numberOfURL int," +
                "PRIMARY KEY (Client_id, StartTime, EndTime)" +
                ");";
        session.execute(tableCQL);
        tableCQL = "CREATE TABLE LI_keyspace.URLRecords (" +
                "Client_id int, " +
                "TimeStamp timestamp, " +
                "Action text, " +
                "Status text," +
                "Size text," +
                "PRIMARY KEY (Client_id, TimeStamp, Action)" +
                ");";
        session.execute(tableCQL);

        searchCQL = session.prepare("INSERT INTO LI_keyspace.SessionRecords (Client_id, StartTime, EndTime, numberOfAccess, numberOfURL)  VALUES (? ,? ,? ,? ,?);");

    }


    public void insertSession() {

        searchCQL = session.prepare("SELECT Client_id, TimeStamp ,Action FROM LI_keyspace.URLRecords");

        insertSessionCQL = session.prepare("INSERT INTO LI_keyspace.SessionRecords (Client_id, StartTime, EndTime, numberOfAccess, numberOfURL)  VALUES (? ,? ,? ,? ,?);");


        boolean inti = true;
        final AtomicReference<SiteSession> expiredSession = new AtomicReference<>(null);
        HashMap<String, SiteSession> sessions = new LinkedHashMap<String, SiteSession>() {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                SiteSession siteSession = (SiteSession) eldest.getValue();
                boolean shouldExpire = siteSession.isExpired();
                if (shouldExpire) {
                    expiredSession.set(siteSession);
                    SiteSession sessionOutput = expiredSession.get();
                    Date startdate = new Date(sessionOutput.getFirstHitMillis());
                    Date enddate = new Date(sessionOutput.getLastHitMillis());
                    int numofaccess = (int) sessionOutput.getHitCount();
                    int numofurl = (int) sessionOutput.getHyperLogLog().cardinality();
                    ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(insertSessionCQL).bind(Integer.parseInt(sessionOutput.getId()), startdate, enddate, numofaccess, numofurl));
                    // sessions.remove(id);
                    // count++;

                    //if(count % 100000 == 0){
                    // System.out.println(count + " of session has been input");
                    //}

                }
                return siteSession.isExpired();
            }
        };


        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchCQL));
        ResultSet rset = queryFuture.getUninterruptibly();
        // SiteSession ss = new SiteSession();
        for (Row row : rset) {

            int cid = row.getInt(0);
            String id = cid + "";
            Date date = row.getDate(1);

            long firstHitMillis = date.getTime();
            String url = row.getString(2);
            if (sessions.containsKey(id) == false) {
                SiteSession ss = new SiteSession();
                ss.setID(id);
                ss.setFirstHitMillis(firstHitMillis);
                ss.update(firstHitMillis, url);
                ss.addHitCount(firstHitMillis, url);

                sessions.put(id, ss);


            } else {
                SiteSession s1 = sessions.get(id);
                SiteSession.setGlobalLastHitMillis(firstHitMillis);
                if (s1.isExpired()) {

                    SiteSession sessionOutput = s1;
                    Date startdate = new Date(sessionOutput.getFirstHitMillis());
                    Date enddate = new Date(sessionOutput.getLastHitMillis());
                    int numofaccess = (int) sessionOutput.getHitCount();
                    int numofurl = (int) sessionOutput.getHyperLogLog().cardinality();
                    ResultSetFuture addSessionQueryFuture = session.executeAsync(new BoundStatement(insertSessionCQL).bind(Integer.parseInt(id), startdate, enddate, numofaccess, numofurl));
                    sessions.remove(id);


                } else {
                    s1.addHitCount(firstHitMillis, url);
                    sessions.put(id, s1);
                }
            }

        }


    }


    public void getSessionDetails(int clientID) {
        searchSessionByIDCQL = session.prepare("SELECT StartTime,EndTime,numberOfAccess,numberOfURL FROM keyspace.SessionRecords WHERE Client_id = ?");

        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchSessionByIDCQL).bind(clientID));
        ResultSet rset = queryFuture.getUninterruptibly();
        System.out.println("here is the result of " + clientID);
        for (Row row : rset) {
            Date startdate = row.getDate(0);
            Date enddate = row.getDate(1);
            int numofaccess = row.getInt(2);
            int numofurl = row.getInt(3);

            System.out.println("start time: " + startdate + "End time: " + enddate + "number of Access: " + numofaccess + "number of URL access" + numofurl);


        }

    }


    public void close() {
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
