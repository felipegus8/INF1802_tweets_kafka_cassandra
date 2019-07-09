import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import twitter4j.GeoLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TweetRepository {
    private static final String TABLE_NAME = "tweets";
    private static final String TABLE_NAME_BY_LANG = TABLE_NAME + "ByLanguage";
    private Session session;
    public TweetRepository(Session session) { this.session = session;}

    public void createTable() {
        System.out.println("createTable --init");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("id bigint PRIMARY KEY, ")
                .append("username text,")
                .append("text text,")
                .append("created_at text,")
                .append("source text,")
                .append("isTruncated boolean,")
                .append("isFavourite boolean,")
                .append("geo_location_latitude float,")
                .append("geo_location_longitude float,")
                .append("lang text,")
                .append("country text,")
                .append("contributors text);");

        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("createTable --end\n");
    }

    public void createTableTweetsByLanguage() {
        System.out.println("createTableTweetsByLanguage --init");
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_LANG).append("(")
                .append("id bigint, ")
                .append("username text,")
                .append("text text,")
                .append("created_at text,")
                .append("source text,")
                .append("isTruncated boolean,")
                .append("isFavourite boolean,")
                .append("geo_location_latitude float,")
                .append("geo_location_longitude float,")
                .append("lang text,")
                .append("country text,")
                .append("contributors text, PRIMARY KEY((id,lang)));");
        final String query = sb.toString();
        System.out.println(sb);

        session.execute(query);
        System.out.println("createTableTweetsByLanguage --end\n");
    }



    public void inserttweet(Tweet tweet) {
        System.out.println("inserttweet --init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append(" (id,username,text,created_at,source,isTruncated,isFavourite,geo_location_latitude,geo_location_longitude,lang,country,contributors) ")
                .append("VALUES ( ").append(tweet.getId()).append(", '")
                .append(tweet.getUsername()).append("', '")
                .append(tweet.getText()).append("', '")
                .append(tweet.getCreated_at()).append("', '")
                .append(tweet.getSource()).append("', ")
                .append(tweet.isTruncated()).append(", ")
                .append(tweet.isFavorited()).append(", ")
                .append(tweet.getGeoLocation().getLatitude()).append(", ")
                .append(tweet.getGeoLocation().getLongitude()).append(", '")
                .append(tweet.getLang()).append("', '")
                .append(tweet.getCountry()).append("', '")
                .append((tweet.getContributors())).append("');");
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("inserttweet --end");
    }

    public void inserttweetByLanguage(Tweet tweet) {
        System.out.println("inserttweetByLanguage --init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_LANG).append(" (id,username,text,created_at,source,isTruncated,isFavourite,geo_location_latitude,geo_location_longitude,lang,country,contributors) ")
                .append("VALUES ( ").append(tweet.getId()).append(", '")
                .append(tweet.getUsername()).append("', '")
                .append(tweet.getText()).append("', '")
                .append(tweet.getCreated_at()).append("', '")
                .append(tweet.getSource()).append("', ")
                .append(tweet.isTruncated()).append(", ")
                .append(tweet.isFavorited()).append(", ")
                .append(tweet.getGeoLocation().getLatitude()).append(", ")
                .append(tweet.getGeoLocation().getLongitude()).append(", '")
                .append(tweet.getLang()).append("', '")
                .append(tweet.getCountry()).append("', '")
                .append((tweet.getContributors())).append("');");
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("inserttweetByLanguage --end\n");
    }




    public List<Tweet> selectAll() {
        System.out.println("select all --init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME);
        final String query = sb.toString();
        System.out.println(sb);
        ResultSet rs = session.execute(query);
        List <Tweet> tweets = new ArrayList<Tweet>();
        for (Row r:rs) {
            System.out.println(r);
            GeoLocation geo = new GeoLocation(r.getFloat("geo_location_latitude"),r.getFloat("geo_location_longitude"));
            Tweet s = new Tweet(r.getLong("id"),r.getString("username"),r.getString("text"),
                    r.getString("created_at"),r.getString("source"),r.getBool("isTruncated"),r.getBool("isFavourite"),
                    geo,r.getString("lang"),r.getString("contributors"),r.getString("country"));
            tweets.add(s);
        }
        System.out.println("select all --end\n");
        return tweets;
    }

    public List<Tweet> selectAllFromByLanguage() {
        System.out.println("selectAllFromByLanguage--init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_LANG);
        final String query = sb.toString();
        System.out.println(sb);
        ResultSet rs = session.execute(query);
        List <Tweet> tweets = new ArrayList<Tweet>();
        for (Row r:rs) {
            System.out.println(r);
            GeoLocation geo = new GeoLocation(r.getFloat("geo_location_latitude"),r.getFloat("geo_location_longitude"));
            Tweet s = new Tweet(r.getLong("id"),r.getString("username"),r.getString("text"),
                    r.getString("created_at"),r.getString("source"),r.getBool("isTruncated"),r.getBool("isFavourite"),
                    geo,r.getString("lang"),r.getString("contributors"),r.getString("country"));
            tweets.add(s);
        }
        System.out.println("selectAllFromByLanguage --end\n");
        return tweets;
    }

    public List<Tweet> selectByLanguage(String language) {
        System.out.println("selectByLanguage --init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_LANG).append(" WHERE lang='").append(language).append("' ALLOW FILTERING");
        final String query = sb.toString();
        System.out.println(sb);
        ResultSet rs = session.execute(query);
        List <Tweet> tweets = new ArrayList<Tweet>();
        for (Row r:rs) {
            System.out.println(r);
            GeoLocation geo = new GeoLocation(r.getFloat("geo_location_latitude"),r.getFloat("geo_location_longitude"));
            Tweet s = new Tweet(r.getLong("id"),r.getString("username"),r.getString("text"),
                    r.getString("created_at"),r.getString("source"),r.getBool("isTruncated"),r.getBool("isFavourite"),
                    geo,r.getString("lang"),r.getString("contributors"),r.getString("country"));
            tweets.add(s);
        }
        System.out.println("selectByLanguage --end\n");
        return tweets;
    }


    public void deletetweet(long id) {
        System.out.println("deleteTweet --init");
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(TABLE_NAME).append(" WHERE id = ").append(id).append(";");
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("deleteTweet --end\n");
    }

    public void deletetweetByLanguage(long id,String language) {
        System.out.println("deletetweetByLanguage --init");
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_LANG).append(" WHERE id = ").append(id).append(" AND lang = '").append(language).append("';");
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("deletetweetByLanguage--end\n");
    }

    public void deleteTable(String tableName) {
        System.out.println("deleteTable --init");
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("deleteTable --end\n");
    }
}
