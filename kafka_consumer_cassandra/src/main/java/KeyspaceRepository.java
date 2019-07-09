import com.datastax.driver.core.Session;
public class KeyspaceRepository {
    private Session session;
    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName, String replicationStrategy,int numberOfReplicas) {
        System.out.println("create Keyspace --init");
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName).append(" WITH replication = {").append("'class':'")
                .append(replicationStrategy)
                .append("','replication_factor':")
                .append(numberOfReplicas).append("};");
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("create Keyspace --end \n");
    }

    public void useKeyspace(String keyspace) {
        System.out.println("use Keyspace --init ");
        session.execute("USE " + keyspace);
        System.out.println("use Keyspace --end\n");
    }

    public void deleteKeyspace(String keyspaceName) {
        System.out.println("delete Keyspace --init");
        StringBuilder sb = new StringBuilder("DROP KEYSPACE ").append(keyspaceName);
        final String query = sb.toString();
        System.out.println(sb);
        session.execute(query);
        System.out.println("delete Keyspace --end\n");
    }
}