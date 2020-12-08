import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Stack;

public class Post {

    private final String articleLink;
    private final String connectionLink;
    private final String referrer;
    private final String userAgent;
    private final String commentLink;
    private final String SITE;
    private final String TITLE;
    private final String POSTER;
    private int numComments;
    private int numVotes;
    private int topRank;
    private ArrayList<Comment> comments=new ArrayList<>();
    private String enterTime;




    public Post(Elements a, Element b, String link, String userAgent, String referrer, int topRank, boolean firstLogging) throws IOException {
        this.connectionLink =link;
        this.userAgent =userAgent;
        this.referrer =referrer;

        if(!firstLogging){
            this.topRank=topRank;
            ZonedDateTime enterTimeToFormat=ZonedDateTime.now(ZoneId.of("Z"));
            enterTime=DateTimeFormatter.ISO_DATE_TIME.format(enterTimeToFormat);
        }else if (topRank==1){
            enterTime="0";
            this.topRank=topRank;
        }else{
            this.topRank=0;
            enterTime="0";
        }
            SITE = a.last().text().replace("'","''");
            articleLink = a.eq(1).attr("href");

        TITLE = a.eq(1).text().replace("'","''");
        System.out.println(topRank+ ". "+TITLE);
        System.out.println("articleLink with href: "+articleLink);

        String toParse = b.text();
            String[] split = toParse.split(" ");
            numVotes = Integer.parseInt(split[0]);
            numComments = (split[10].equals("discuss") ? 0 : Integer.parseInt(split[10]));
            POSTER = split[3];


//        System.out.println("NumVotes: "+numVotes);
//        System.out.println("NumComments: "+numComments);
//        System.out.println(topRank+". "+ TITLE);
//        System.out.println("enterTime : "+ enterTime);
        this.commentLink= link + b.getElementsByTag("a").eq(1).toString().split("\"")[1];
        System.out.println("commentLink :" + commentLink);
        if (numComments!=0) updateComments();
        System.out.println("========");

    }


    private void updateComments() throws IOException {


//        if(true) throw new HttpStatusException("test non-502", 503, "url"); //testing




        comments.clear();

            String currentPageComments = commentLink;

            int width;
            boolean hasNextPage = false;
            do {
                Document commentDoc = Jsoup.connect(currentPageComments)
                        .userAgent(userAgent)
                        .referrer(referrer)
                        .get();


                Elements temp = commentDoc.select("tr.athing.comtr");

                for (Element e : temp) {
                    String comment = e.select("span.commtext").text();
                    width = (Integer.parseInt(e.select("td.ind").select("img").attr("width")) / 40) + 1;
                    int length = comment.length();

                    if (length >= 6) {
                        if (comment.substring(length - 6, length).equals(" reply"))
                            comment = comment.substring(0, length - 6);
                    }
                    String toParse2 = e.select("span.comhead").first().text();
                    int i = toParse2.indexOf(" ");
                    String commenter = (toParse2.substring(0, i));


                    comments.add(new Comment(comment, commenter, width));
//                        System.out.print(width==1? "Parent ": "Child ");
//                        System.out.println("Comment by "+commenter+": "+comment);
                }

                String moreLink = (commentDoc.select("a.morelink").attr("href"));
                currentPageComments = connectionLink + moreLink;
                hasNextPage = moreLink.length() != 0;


            } while (hasNextPage);

    }



    //builds transaction that logs posts and all comments
    int reconcile(int id, int commentID, Connection conn) throws SQLException, UnexpectedException {
        ZonedDateTime exitTime= ZonedDateTime.now(ZoneId.of("Z"));
        String exit=DateTimeFormatter.ISO_DATE_TIME.format(exitTime);


        System.out.println("comments.size(): "+comments.size());
        int numCommentsCycled=0;
        try{
            conn.setAutoCommit(false);
            Stack<Comment> stack=new Stack<>();
            for(Comment comment: comments){
                numCommentsCycled++;
//                System.out.println(comment.getTEXT());

                comment.getInsertStatement(conn, ++commentID, id, stack).execute();
            }

            prepareInsert(conn, id, enterTime, exit).execute();
            conn.commit();

        }catch(Exception e){
            conn.rollback();
            e.printStackTrace();
            throw new UnexpectedException(ExceptionUtils.getStackTrace(e));

        }
        System.out.println("numCommentsCycled in Post.reconcile: "+numCommentsCycled);

        return commentID;
    }

    public void update(Post moreRecent) throws IOException {

        this.numComments= Math.max(this.numComments, moreRecent.numComments);
        if(this.topRank!=0) this.topRank= Math.max(this.topRank, moreRecent.topRank);
        this.numVotes= Math.max(this.numVotes, moreRecent.numVotes);
        if(this.numComments!=0 && moreRecent.numComments!=0) updateComments();

    }



    //prepare a statement to log post, to be executed in transaction with comments
    private PreparedStatement prepareInsert(Connection conn, int id, String enterTime, String exitTime) throws SQLException {
        String parsedTitle=TITLE.replace("'","''");

        StringBuilder sb=new StringBuilder("INSERT INTO posts (id, postTitle, postSite, posterName, points, topRank, entryTime, exitTime, numComments, articleLink, commentLink) VALUES (");
        sb
                .append(id).append(",")
                .append("'").append(parsedTitle).append("'").append(",")
                .append("'").append(SITE).append("'").append(",")
                .append("'").append(POSTER).append("'").append(",")
                .append(numVotes).append(",")
                .append(topRank).append(",")
                .append("'").append(enterTime).append("'").append(",")
                .append("'").append(exitTime).append("'").append(",")
                .append(numComments).append(",")
                .append("'").append(articleLink).append("'").append(",")
                .append("'").append(commentLink).append("'")
                .append(");\n");

        System.out.println("numComments in post.prepareInsert: "+numComments);
        System.out.println("numVotes in post.prepareInsert: "+numVotes);


        System.out.println(sb.toString());
        return conn.prepareStatement(sb.toString());
    }

    public String getTITLE() {
        return TITLE;
    }
    public String getPOSTER() { return POSTER;}
    public String getCommentLink() {
        return commentLink;
    }

    //Considered same post if it has same title and poster
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Post)) return false;
        return this.TITLE.equals(((Post)obj).TITLE) && this.POSTER.equals((((Post) obj).POSTER));
    }

    @Override
    public String toString() {
        return this.TITLE;
    }
}
