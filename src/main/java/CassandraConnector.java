import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnector {
    private Cluster cluster;

    private Session session;

    private String keyspace;

    public void setKeyspace(String keyspace){
        this.keyspace = keyspace + ".";
    }

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }


    public void deleteTable(String tableName) {
        StringBuilder sb =
                new StringBuilder("DROP TABLE IF EXISTS ").append(keyspace).append(tableName);

        String query = sb.toString();
        session.execute(query);
    }

    public void deleteKeyspace(String keyspaceName) {
        StringBuilder sb =
                new StringBuilder("DROP KEYSPACE ").append(keyspaceName);

        String query = sb.toString();
        session.execute(query);
    }

    public void createTable(String tableName, String columns){
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keyspace).append(tableName).append(" ( ")
                .append(columns).append(" );");
        String query = sb.toString();
        session.execute(query);
    }





}
