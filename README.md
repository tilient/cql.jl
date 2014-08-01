cql.js
======

cassandra cql for julia

API
===

    function connect(srv::String = "localhost", prt::Int = 9042)

    function disconnect(con::CQLConnection)


    function query(con::CQLConnection, msg::String)

'Normal' Synchronous Query
Will wait for until all scheduled commands have been executed
Will then send the query and wait for the result.
The processed result is returned as an array or
rows, which are themselves array with the values
for the requested columns.

    function command(con::CQLConnection, msg::String)

The same as 'query', but we don't get the result back.
Is a bit faster and uses less memory, because the
reply from the server is neglected.

    function asyncQuery(con::CQLConnection, msg::String)

An Asynchronous Query
Will send the query to the server and returns 
with a 'future'. After the server has processed the
query and did send back the result, the 'future' will
contain the result. This result can be fetched with
'getResult', which gives back the result in 
the same format as 'query'.

    function asyncCommand(con::CQLConnection, msg::String)

The same as 'command', but asynchronous.
It sends of the command to the server instantly and
neglects the response.
This is the fastest way to execute cql commands, but
no garantees can be given on e.g. the order in which
commands are being executed by the server.

    function getResult(reply)

To fetch the result from a call by asyncQuery.
Will block if the result is not there yet.

    function sync(con)

Not very usefull, but waits until all messages
that were send to the server where processed and
communicated back.
Can be handy to synchronize or do correct timig tests.

