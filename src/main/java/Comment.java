import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Stack;

public class Comment {
    private final String POSTER;
    private final String TEXT;
    private final int width;
    //primary key identifier, not to be confused with postID which relates it to a post or commentID which isn't a field
    //but inserts into the database the id field of it's parent comment if there is one
    private int id;

    public Comment(String text, String poster, int width){
//        int z=10/0;
        this.TEXT =text;
        this.POSTER =poster;
        this.width=width;
    }


    //returns UNSAFE text
    public String getTEXT() {
        return TEXT;
    }


    public PreparedStatement getInsertStatement(Connection conn, int id, int postID, Stack<Comment> commentChain)
            throws SQLException, UnexpectedException {
        this.id=id;
        int parentCommentID=-1;

        //Maintains a stack of comments that mimics the visual representation on the site to relate comments
        //Only maintains comments that may have a reply to them in the future

        //peek then push happens in all else statements, so can remove the peek/assert/push and drop
        if(this.width==1){
            commentChain.clear();

        }else{
            if (commentChain.peek().width>=this.width){
                while(commentChain.peek().width>=this.width) commentChain.pop();
            }else if (commentChain.peek().width!=this.width-1) throw new UnexpectedException("Comments not in expected order. Top of comment chain width: "+commentChain.peek().width+"." +
                    "this width: "+this.width+". This comment: "+commentChain.peek().getTEXT());

            parentCommentID=commentChain.peek().id;
            assert parentCommentID!=-1;
        }
        commentChain.push(this);


        String parsedText= TEXT.replace("'","''");

        StringBuilder sb=new StringBuilder("INSERT INTO comments(id, postID, posterName, commentText");

        boolean isParentComment=(parentCommentID==-1);

        if(!isParentComment){
            sb.append(", parentCommentID");
        }

                sb.append(") VALUES (")
                        .append(id).append(",")
                .append(postID).append(",")
                .append("'").append(POSTER).append("'").append(",")
                .append("'").append(parsedText).append("'");

        if(!isParentComment){
            sb.append(",").append(parentCommentID);
        }
        sb.append(");\n");


        System.out.println(sb.toString());

        return conn.prepareStatement(sb.toString());
    }



    @Override
    public boolean equals(Object obj) {
        if(obj.getClass()!=this.getClass()) return false;
        return this.POSTER.equals(((Comment) obj).POSTER) && this.TEXT.equals(((Comment) obj).TEXT);
    }

    @Override
    public String toString() {
        return ((this.width==1? "Parent ": "Child ") +"comment by "+POSTER+":\n"+TEXT);
    }
}
