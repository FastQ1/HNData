import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
Stores in memory a list of Post objects which in turn have their own comment objects.
Updates list every so often(currently set @5 minutes+time of operations).
When a cycle recognizes that the list of Posts varies from the last list, it logs
any posts contained in the previous list- these represent posts that left the front page
*/
public class TempDB {

    private static TempDB tempDB;
    private List<Post> posts = new ArrayList<>();
    private static final String HACKERNEWS_LINK = "https://news.ycombinator.com/";
    private static final String USER_AGENT = ConnectionVariables.getUserAgent();
    private static final String REFERRER_GOOGLE = "https:/www.google.com";
    private static int dbID;
    private static int commentID;
    private static String CONNECTION_STRING;
    //Preserves data integrity by not logging entry time/top rank data if the program is being started
    //from fresh- instead entering null values in these fields for posts which entered the first list in memory.
    private static boolean firstCycle = true;
    private final Logger logger;

    enum Database {
        MSSQLSERVER,
        SQLITE_HOME,
//        SQLITE_VM
    }

    //Unnecessary singleton
    public static TempDB getInstance(Database db) {
        try {
            if (tempDB == null) tempDB = new TempDB(db);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return tempDB;
    }


    private TempDB(Database db) throws SQLException {
        if (db.equals(Database.MSSQLSERVER)) CONNECTION_STRING = ConnectionVariables.getSqlServerConnectionString();
        else if (db.equals(Database.SQLITE_HOME))
            CONNECTION_STRING = ConnectionVariables.getSqliteHomeConnectionString();
        else throw new IllegalStateException("Connection enum not as expected");

        logger = new Logger(CONNECTION_STRING);

        try {
            establish();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void establish() throws SQLException, InterruptedException {
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {
            if (conn != null) System.out.println("Connected");
            else throw new InterruptedException("Connection not found in tempDB.establish()");


            dbID = findLastEntry(conn);
            commentID = findLastCommentEntry(conn);

            while (true) {
                logger.cycle();

                Document doc = Jsoup.connect(HACKERNEWS_LINK)
                        .userAgent(USER_AGENT)
                        .referrer(REFERRER_GOOGLE)
                        .get();

                Elements temp = doc.select("tr.athing");

                int rank = 1;
                List<Post> newPage = new ArrayList<>();
                for (Element e : temp) {
                    Elements a = e.getElementsByTag("a");
                    Element b = e.nextElementSibling();
                    int i = a.size();

                    //Leave out text posts and ads (2 elements, sometimes 1), leave in 3 element Elements, throw exception if different #
                    if (i == 3) {
                        //query the list to see if the element is inside, if not add it
                        //pass a boolean parameter to constructor to indicate
                        Post post = new Post(a, b, HACKERNEWS_LINK, USER_AGENT, REFERRER_GOOGLE, rank++, firstCycle);

                        if (posts.contains(post)) {
                            int index = posts.indexOf(post);
                            posts.get(index).update(post);

                        } else {
                            posts.add(post);
                        }
                        newPage.add(post);

                        //most exceptions in this program propagate and terminate to protect data.
                        //These exceptions don't really happen though, it ends up being stable.
                    } else if (i != 2 && i != 1) throw new UnsupportedOperationException();

                    else rank++;

                }

                //Saves posts that left front page
                List<Post> reconciled = new ArrayList<>(posts);
                reconciled.removeAll(newPage);
                for (Post post : reconciled) reconcile(post, conn);


                firstCycle = false;
                Thread.sleep(300_000);

            }

            //This 502 status exception was a harmless one from the page going down very temporarily. I've chosen
            //to catch and retry despite the general model of the program being to always terminate and avoid logging bad data
        } catch (HttpStatusException e) {
            logger.bug(e);
            e.printStackTrace();
            if (e.getStatusCode() == 502) {
                Thread.sleep(300_000);
                establish();
            } else logger.close();
        } catch (Exception e) {
            logger.bug(e);
            e.printStackTrace();
            logger.close();

        }
    }


    //Multiple connections not great practice but necessary to maintain transaction and clear code in my judgement
    private void reconcile(Post post, Connection conn) throws Exception {
        boolean isNewPost = true;
        logger.query("duplicateCheck");
        PreparedStatement statement = conn.prepareStatement("SELECT postTitle, posterName from posts WHERE id>(SELECT MAX(id) FROM posts)-100");
        ResultSet rs = statement.executeQuery();
        String title = post.getTitle();
        String poster = post.getPoster();

        while (rs.next()) {
            if (rs.getString(1).equals(title) && rs.getString(2).equals(poster)) {
                isNewPost = false;
                break;
            }
        }


        logger.reconcile(post, isNewPost);
        if (isNewPost) {
            System.out.println("Reconciling " + post);
            commentID = post.reconcile(++dbID, commentID, conn);
        } else {
            System.out.println("Did not reconcile post due to already existing in DB: " + post);
        }
        posts.remove(post);
    }

    //gets id of last entry in the DB while beginning the program. Chosen over auto incrementing
    //due to how the program relates comments with posts and with each other
    private int findLastEntry(Connection conn) throws Exception {
        PreparedStatement statement = conn.prepareStatement("SELECT id FROM posts WHERE id = (SELECT MAX(id) FROM posts);");
        ResultSet rs = statement.executeQuery();
        rs.next();
        int id=rs.getInt(1);
        System.out.println("Last post id according to findLastEntry: " + id);
        return id;

    }
    //A bit of repeating myself for readability
    private int findLastCommentEntry(Connection conn) throws Exception {
        PreparedStatement statement2 = conn.prepareStatement("SELECT id FROM comments WHERE id= (SELECT MAX(id) FROM comments);");
        ResultSet rs = statement2.executeQuery();
        rs.next();
        int id = rs.getInt(1);
        System.out.println("Last comment id according to findLastCommentEntry: " + id);
        return id;
    }

}
