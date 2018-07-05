import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class main {
    public static void main(String[] args) throws InterruptedException {
        //starting cassandra: 'docker run --name container-name -p localport:9042 cassandra:latest'
        //add node to cluster: 'docker run --name another-container-name -d --link container-name:cassandra cassandra:latest
        int replicationfactor = 1;
        String keyspace = "kspace";
        String[][] tabellen = {
                {"personen", "id int primary key, vorname text, name text, age int, adresseId int"},
                {"adressen", "id int primary key, strasse text, nr int, zusatz text, stadtId int"},
                {"staedte", "id int primary key, ort text"}
        };

        //Verbindung aufbauen
        CassandraConnector client = new CassandraConnector();
        client.connect("localhost" ,9042 );
        Session session = client.getSession();

        //Keyspace erstellen
        client.createKeyspace(keyspace, "SimpleStrategy", replicationfactor);
        //Keyspace nutzen
        client.setKeyspace(keyspace);

        //Tabellen erstellen
        for(String[] s :tabellen){
            client.createTable(s[0], s[1]);
        }


        //Data einfügen
        final String insertQuery = "Insert into %s.%s (%s) values(%s);";
        List<String> inserts = new ArrayList<>();
        String table = tabellen[0][0];
        inserts.add(String.format(insertQuery, keyspace, table, "id, vorname, name, age, adresseId", "0, 'Peter', 'Zwaegat', 73, 0"));
        inserts.add(String.format(insertQuery, keyspace, table, "id, vorname, name, age, adresseId", "1, 'Max', 'Mustermann', 26,1"));
        inserts.add(String.format(insertQuery, keyspace, table, "id, vorname, name, age, adresseId", "2, 'Melinda', 'Musterfrau', 25, 1"));

        table = tabellen[1][0];
        inserts.add(String.format(insertQuery, keyspace, table, "id, strasse, nr, zusatz, stadtId", "0, 'Dorfstrasse', 21,'' ,0 "));
        inserts.add(String.format(insertQuery, keyspace, table, "id, strasse, nr, zusatz, stadtId", "1, 'Falkenstrasse', 126, 'A', 1"));

        table = tabellen [2][0];
        inserts.add(String.format(insertQuery, keyspace, table, "id, ort", "0, 'Lübeck'"));
        inserts.add(String.format(insertQuery, keyspace, table, "id, ort", "1, 'Hamburg'"));

        for(String query: inserts ){
            session.execute(query);
        }



        //Select statements
        String selectQuery = "Select %s from %s.%s"; //column, keyspace, table
        List<String> selects = new ArrayList<>();
        selects.add(String.format(selectQuery, "*", keyspace, "personen"));
        selects.add(String.format(selectQuery, "*", keyspace, "adressen"));
        selects.add(String.format(selectQuery, "*", keyspace, "staedte"));

        for(String s : selects){
            ResultSet res = session.execute(s);
            System.out.println(res.getColumnDefinitions().toString());
            res.all().stream().forEach(e -> System.out.println(e.toString()));
            System.out.println();
        }



        Thread.sleep(120000);
        //Tabellen löschen
        for(String[] s :tabellen){
            client.deleteTable(s[0]);
        }

        //Keyspace löschen
        client.deleteKeyspace(keyspace);


        //verbindung abbauen
        client.close();


    }
}
