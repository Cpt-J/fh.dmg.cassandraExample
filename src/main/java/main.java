import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class main {

   static int replicationfactor = 1;
    static String keyspace = "kspace";
    static String[][] tabellen = {
            {"personen", "id int primary key, vorname text, name text, age int, adresseId int"},
            {"adressen", "id int primary key, strasse text, nr int, zusatz text, stadtId int"},
            {"staedte", "id int primary key, ort text"}
    };
   static  CassandraConnector client;
    public static void main(String[] args) throws InterruptedException {
        //starting cassandra: 'docker run --name container-name -p localport:9042 -d cassandra:latest'
        // get ip: 'docker inspect -f '{{.NetworkSettings.IPAddress}}' container-name'
        //docker run -e CASSANDRA_SEEDS="$('docker inspect -f '{{.NetworkSettings.IPAddress}}' container-name)" -d --name ...
        //add node to cluster: 'docker run --name another-container-name -d --link container-name:cassandra cassandra:latest


        //Verbindung aufbauen
        client = new CassandraConnector();
        client.connect("localhost" ,9042 );

        //Keyspace erstellen
        client.createKeyspace(keyspace, "SimpleStrategy", replicationfactor);
        //Keyspace nutzen
        client.setKeyspace(keyspace);



        Scanner in = new Scanner(System.in);
        char c = 'a';
        do {
            System.out.println("1: Tabellen erstellen");
            System.out.println("2: Datensätze einfügen");
            System.out.println("3: Datensätze anzeigen");
            System.out.println("4: Tabellen löschen");
            System.out.println("q: beenden");
            c = in.next().charAt(0);
            switch (c){
                case '1':
                    createTable();
                   break;
                case '2':
                    insertData();;
                    break;
                case '3':
                    showData();
                    break;
                case '4':
                    removeTables();
                    break;
            }
        }while(c != 'q');

        in.close();


        //verbindung abbauen
        client.close();


    }

    public static void removeTables(){
        //Tabellen löschen
        for(String[] s :tabellen){
            client.deleteTable(s[0]);
        }
    }

    public static void showData(){
        //Select statements
        String selectQuery = "Select %s from %s.%s"; //column, keyspace, table
        List<String> selects = new ArrayList<>();
        selects.add(String.format(selectQuery, "*", keyspace, "personen"));
        selects.add(String.format(selectQuery, "*", keyspace, "adressen"));
        selects.add(String.format(selectQuery, "*", keyspace, "staedte"));

        for(String s : selects){
            ResultSet res = client.getSession().execute(s);
            System.out.println(res.getColumnDefinitions().toString());
            res.all().stream().forEach(e -> System.out.println(e.toString()));
            System.out.println();
        }
    }

    public static void createTable(){
        //Tabellen erstellen
        for(String[] s :tabellen){
            client.createTable(s[0], s[1]);
        }
    }
    public static void createKeyspace(){

    }
    public static void insertData(){
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
            client.getSession().execute(query);
        }
    }

}
