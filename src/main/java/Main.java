import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

/**
 * Created by fanchenli on 2/13/14.
 */
public class Main {

    public static void main(String[] args){
        ReadContent rd = new ReadContent();

        try {
            rd.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        ClientLi cl  = new ClientLi();
        cl.CreateSessionTable();
        cl.insertSession();
        cl.getSessionDetails(34600);
        cl.close();
    }

}
