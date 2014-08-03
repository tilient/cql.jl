using cql;

server = "tilient.net";

function test01()
  ## 'Example of a music service' from 
  ## the manual 'About CQL for Cassandra 2.0'

  c = cql.connect(server);

  cql.command(c, 
    "CREATE KEYSPACE demo 
       WITH REPLICATION = {'class' : 'SimpleStrategy',
                           'replication_factor' : 1};");
  cql.command(c, "USE DEMO;");

  cql.command(c,
    "CREATE TABLE songs (id     uuid PRIMARY KEY,
                         title  text,
                         album  text,
                         artist text,
                         data   blob );");

  cql.command(c,
    "CREATE TABLE playlists (id         uuid,
                             song_order int,
                             song_id    uuid,
                             title      text,
                             album      text,
                             artist     text,
                             PRIMARY KEY (id, song_order));");

  cql.command(c,
    "INSERT INTO playlists (id, song_order, song_id, 
                            title, artist, album)
            VALUES (62c36092-82a1-3a00-93d1-46196ee77204, 1,
                    a3e64f8f-bd44-4f28-b8d9-6938726e34d4, 
                    'La Grange', 'ZZ Top', 'Tres Hombres');");
  cql.command(c,
    "INSERT INTO playlists (id, song_order, song_id, 
                           title, artist, album)
           VALUES (62c36092-82a1-3a00-93d1-46196ee77204, 2,
                   8a172618-b121-4136-bb10-f665cfc469eb, 
                   'Moving in Stereo', 'Fu Manchu', 
                   'We Must Obey'); ");
  cql.command(c,
    "INSERT INTO playlists (id, song_order, song_id, 
                           title, artist, album)
           VALUES (62c36092-82a1-3a00-93d1-46196ee77204, 3,
                   2b09185b-fb5a-4734-9b56-49077de9edbf, 
                   'Outside Woman Blues', 'Back Door Slam', 
                   'Roll Away'); ");
  cql.command(c,
    "INSERT INTO playlists (id, song_order, song_id, 
                            title, artist, album)
           VALUES (62c36092-82a1-3a00-93d1-46196ee77204, 4,
                   7db1a490-5878-11e2-bcfd-0800200c9a66,
                   'Ojo Rojo', 'Fu Manchu', 
                   'No One Rides for Free'); ");

  println(cql.query(c, 
    "SELECT * FROM playlists;"));
 
  cql.command(c, 
    "CREATE INDEX ON playlists(artist);");
                
  println(cql.query(c, 
    "SELECT * FROM playlists WHERE artist = 'Fu Manchu';"));
                
  println(cql.query(c,
    "SELECT * FROM playlists 
     WHERE id = 62c36092-82a1-3a00-93d1-46196ee77204 
     ORDER BY song_order DESC 
     LIMIT 50; "));
                    
  cql.command(c, 
    "ALTER TABLE songs ADD tags set<text>;");
                    
  cql.command(c,
    "UPDATE songs  SET tags = tags + {'2007'}
       WHERE id = 8a172618-b121-4136-bb10-f665cfc469eb; "); 
     
  cql.command(c,
    "UPDATE songs  SET tags = tags + {'covers'}
       WHERE id = 8a172618-b121-4136-bb10-f665cfc469eb; "); 
     
  cql.command(c,
    "UPDATE songs  SET tags = tags + {'1973'}
       WHERE id = a3e64f8f-bd44-4f28-b8d9-6938726e34d4; "); 
     
  cql.command(c,
    "UPDATE songs  SET tags = tags + {'blues'}
       WHERE id = a3e64f8f-bd44-4f28-b8d9-6938726e34d4; "); 
     
  cql.command(c,
    "UPDATE songs  SET tags = tags + {'rock'}
       WHERE id = 7db1a490-5878-11e2-bcfd-0800200c9a66; "); 
  
  cql.command(c, 
    "ALTER TABLE songs ADD reviews list<text>;");

  cql.command(c, 
    "ALTER TABLE songs ADD venue map<timestamp, text>;");
  
  cql.command(c,
    "UPDATE songs
       SET tags = tags + {'rock'}
       WHERE id = 7db1a490-5878-11e2-bcfd-0800200c9a66;");

  cql.command(c,
    "UPDATE songs
       SET reviews = reviews + [ 'hot dance music' ]
       WHERE id = 7db1a490-5878-11e2-bcfd-0800200c9a66;");

  cql.command(c, 
    "INSERT INTO songs (id, venue)
       VALUES (7db1a490-5878-11e2-bcfd-0800200c9a66, 
               { '2013-9-22 12:01' : 'The Fillmore', 
                 '2013-10-1 18:00' : 'The Apple Barrel'});");

  println(cql.query(c, "SELECT * FROM SONGS"));
  println(cql.query(c, "SELECT id, tags FROM songs;"));
  println(cql.query(c, "SELECT id, venue FROM songs;"));

  cql.command(c, 
    "CREATE INDEX album_name ON playlists (album);");
  cql.command(c, 
    "CREATE INDEX title_name ON playlists (title);");

  println(cql.query(c,
    "SELECT * FROM playlists
       WHERE album = 'Roll Away' 
         AND title = 'Outside Woman Blues'
       ALLOW FILTERING;"));
  
  cql.command(c,
    "UPDATE songs
       SET title = 'NN'
       WHERE id = 8a172618-b121-4136-bb10-f665cfc469eb;");
  
  println(cql.query(c, 
    "SELECT WRITETIME (title)
       FROM songs
       WHERE id = 8a172618-b121-4136-bb10-f665cfc469eb;"));
  
  
  ## clean up ##

  cql.command(c, "DROP TABLE songs;");
  cql.command(c, "DROP TABLE playlists;");
  cql.command(c, "DROP KEYSPACE demo;");

  cql.disconnect(c);
  nothing
end

function test02()
  c = cql.connect(server);

  cql.command(c, 
    "CREATE KEYSPACE demo 
       WITH REPLICATION = {'class' : 'SimpleStrategy',
                           'replication_factor' : 1};");
  cql.command(c, "USE DEMO;");
  cql.command(c, 
    "create table person (id int primary key, name varchar);");

  ## Fast, Asynchronous inserts ##
  @time begin
    for id in 1:17000
      cql.asyncCommand(c, 
        "INSERT INTO person (id, name) VALUES ($id, 'hihaho');");
    end
    cql.sync(c);
  end

  println(cql.query(c, "SELECT count(*) from person;")[1][1]);

  cql.command(c, "DROP TABLE person;");
  cql.command(c, "DROP KEYSPACE demo;");
  cql.disconnect(c);
  nothing
end

function test03()
  c = cql.connect(server);

  cql.command(c, 
    "CREATE KEYSPACE demo 
       WITH REPLICATION = {'class' : 'SimpleStrategy',
                           'replication_factor' : 1};");
  cql.command(c, "USE DEMO;");
  cql.command(c, 
    "create table person (id int , od int, name varchar, 
                          PRIMARY KEY (id, od));");

  N = 10;
  
  @time begin
    for i in 1:N
      cql.asyncCommand(c, 
        "INSERT INTO person (id, od, name) 
         VALUES (1, $i, 'hihaho');");
    end
    cql.sync(c);
  end

  @time begin
    cql.query(c, 
      "select id, od from person where id = 1 
         order by od asc limit $N;");
    nothing
  end

  cql.command(c, "DROP TABLE person;");
  cql.command(c, "DROP KEYSPACE demo;");
  cql.disconnect(c);
  nothing
end

##################################################################
