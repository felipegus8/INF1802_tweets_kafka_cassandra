import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.List;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import twitter4j.GeoLocation;
import twitter4j.Place;

public class ConsumerManager implements LifecycleManager,Serializable {

    private static Logger logger = LoggerFactory.getLogger(ConsumerManager.class.getName());
    private boolean isConsuming = false;
    private Thread ConsumerThread;
    Cluster cluster = null;
    KeyspaceRepository sr = null;
    TweetRepository tr = null;
    Session session = null;
    int tweetCounter = 0;


    public void start() {
        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        isConsuming = true;


        // Criar o consumidor
        KafkaConsumer<String ,Tweet> consumer = new KafkaConsumer<String, Tweet>(properties);

        // Subscrever o consumidor para o nosso(s) tópico(s)
        consumer.subscribe(Collections.singleton("twitter_topic_cassandra7"));

        try {
            cluster = Cluster.builder().addContactPoint("localhost").build();
            session = cluster.connect();
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            sr = new KeyspaceRepository(session);
            sr.createKeyspace("twitter","SimpleStrategy",1);

            sr.useKeyspace("twitter");

            tr = new TweetRepository(session);
            tr.createTable();
            tr.createTableTweetsByLanguage();


        // Ler as mensagens
        ConsumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (isConsuming) {
                    ConsumerRecords<String, Tweet> poll = consumer.poll(Duration.ofMillis(10000));
                    System.out.println(poll.count());
                    for (ConsumerRecord record : poll) {
                        System.out.println("Entrou aqui");
                        Tweet readTweet = Tweet.class.cast(record.value());
                        try {
                            tr.inserttweet(readTweet);
                            tr.inserttweetByLanguage(readTweet);
                            tweetCounter += 1;
                        }
                        catch(Exception e) {
                            System.out.println("Erro inserindo tweet");
                            System.out.println(e);
                            isConsuming = false;
                            break;
                        }
                        tr.selectAll();
                        tr.selectAllFromByLanguage();
                        tr.selectByLanguage("en");

                        if(tweetCounter >= 20) {
                            isConsuming = false;
                            break;
                        }

                        logger.info(record.topic() + " - " + record.partition() + " - " + record.value());
                    }

                }
                stop();

            }
        });
        ConsumerThread.start();
        } catch(Exception e){
            System.out.println("ERROU");
            System.out.println(e);
        }
    }


    public void stop() {
        System.out.println("STOP");
        this.isConsuming = false;

        List<Tweet> alltweets = tr.selectAll();
        tr.deletetweet(alltweets.get(0).getId());

        List<Tweet> tweetsFromTableLanguage = tr.selectAllFromByLanguage();
        tr.deletetweetByLanguage(tweetsFromTableLanguage.get(0).getId(),tweetsFromTableLanguage.get(0).getLang());

        List<Tweet> tweetsByCountry = tr.selectByLanguage("es");
        tr.deletetweetByLanguage(tweetsByCountry.get(0).getId(),tweetsByCountry.get(0).getLang());

        tr.deleteTable("tweets");
        tr.deleteTable("tweetsByLanguage");
        if (cluster != null) cluster.close();
        System.exit(0);

    }
}