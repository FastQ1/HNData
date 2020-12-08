import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.rmi.UnexpectedException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TempDB {


    private static TempDB tempDB;
    private ArrayList<Post> posts=new ArrayList<>();
    private static final String HACKERNEWS_LINK="https://news.ycombinator.com/";
    private static final String USER_AGENT=ConnectionVariables.getUserAgent();
    private static final String REFERRER_GOOGLE="https:/www.google.com";
    private static int dbID;
    private static int commentID;

    private static String CONNECTION_STRING;


    //this checks for first cycle, not to be confused with logger
    //which is an eventLogger
    private static boolean firstLogging=true;
    private final Logger logger;
    enum Database{
        MSSQLSERVER,
        SQLITE_HOME,
        SQLITE_VM
    }


//    private final Post[] posts=new Post[30];

//
    public static TempDB getInstance(Database db){
        try{
            if(tempDB==null) tempDB=new TempDB(db);
        }catch (Exception e){
            e.printStackTrace();
            System.exit(0);
        }

        return tempDB;
    }


    private TempDB(Database db) throws SQLException, UnexpectedException {
        if(db.equals(Database.MSSQLSERVER)) CONNECTION_STRING=ConnectionVariables.getSqlServerConnectionString();
        else if (db.equals(Database.SQLITE_HOME)) CONNECTION_STRING=ConnectionVariables.getSqliteHomeConnectionString();
        else throw new UnexpectedException("Connection enum not as expected");

        logger=new Logger(CONNECTION_STRING);

        try{
            establish();
        }catch(Exception e){
            e.printStackTrace();
        }



        }

        private void establish() throws SQLException, InterruptedException {
            try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {
                if(conn!=null) System.out.println("Connected");
                else System.out.println("Error connecting");


                assert conn != null;
                dbID = findLastEntry(conn);
                commentID=findLastCommentEntry(conn);

                while(true) {
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

                        //Leave out text posts and ads (2 elements), leave in 3 element Elements, throw exception if different #
                        if (i == 3) {
                            //query the TempDB to see if the element is inside
                            //pass a boolean parameter to constructor to indicate
                            Post post = new Post(a, b, HACKERNEWS_LINK, USER_AGENT, REFERRER_GOOGLE, rank++, firstLogging);


                            if (posts.contains(post)) {
                                int index = posts.indexOf(post);
                                posts.get(index).update(post);

                            } else {
                                posts.add(post);
                            }

                            newPage.add(post);

                        } else if (i != 2 && i!=1) throw new UnsupportedOperationException();
                            //end program and clear temporary DB
                        else rank++;

                    }

                    //Saves posts that left front page
                    List<Post> reconciled= new ArrayList<>(posts);
                    reconciled.removeAll(newPage);
                    for (Post post: reconciled) reconcile(post, conn);


                    firstLogging = false;
                    Thread.sleep(300_000);

                }
            }catch(HttpStatusException e){
                logger.bug(e);
                e.printStackTrace();
                if(e.getStatusCode()==502){
                    Thread.sleep(300_000);
                    establish();
                }else logger.close();
            }

            catch(Exception e){
                logger.bug(e);
                e.printStackTrace();
                logger.close();

            }
        }


    //update main database with posts that leave the front page
    //Should change this to have all the connection handled in Post object?
    //Not sure how making the extra connections would affect performance though.
    //Probably not much in practice- but it's encapsulation vs executing code multiple times in this case
    private void reconcile(Post post, Connection conn) throws Exception {
        boolean isNewPost=true;
        logger.query("duplicateCheck");
        PreparedStatement statement=conn.prepareStatement("SELECT postTitle, posterName from posts WHERE id>(SELECT MAX(id) FROM posts)-100");
        ResultSet rs=statement.executeQuery();
        String title=post.getTITLE();
        String poster=post.getPOSTER();

        while(rs.next()){
            if(rs.getString(1).equals(title) || rs.getString(2).equals(poster)){
                isNewPost=false;
                break;
            }
        }


        logger.reconcile(post, isNewPost);
        if(isNewPost){
            System.out.println("Reconciling "+post);
            commentID= post.reconcile(++dbID, commentID, conn);
        }else{
            System.out.println("Did not reconcile post due to already existing in DB: "+post);
        }
        posts.remove(post);
    }

    //gets int of last entry in the DB while beginning the program
    private int findLastEntry(Connection conn) throws Exception {
//        PreparedStatement statement=conn.prepareStatement("INSERT INTO comments (id, postID, posterName, commentText, parentCommentID) "
//        +"VALUES (2, 1, 'testy2', 'hi', 1);");
        PreparedStatement statement= conn.prepareStatement("SELECT id FROM posts WHERE id = (SELECT MAX(id) FROM posts);");
//        PreparedStatement statement= conn.prepareStatement("SELECT * FROM posts");
        ResultSet rs=statement.executeQuery();
        rs.next();
        System.out.println("Last post id according to findLastEntry: "+ rs.getInt(1));
        return rs.getInt(1);

    }

    private int findLastCommentEntry(Connection conn) throws Exception{
        PreparedStatement statement2= conn.prepareStatement("SELECT id FROM comments WHERE id= (SELECT MAX(id) FROM comments);");
        ResultSet rs2=statement2.executeQuery();
        rs2.next();

        //clean up this later
        int id2=rs2.getInt(1);
        System.out.println("Last comment id according to findLastCommentEntry: "+id2);
        return id2;
    }

}
