module cql

##################################################################
# CQLConnection
##################################################################

type CQLConnection
  socket  :: Base.TcpSocket
  buffer  :: IOBuffer
  msg_id  :: Uint8
  replies :: Dict
  pending :: Int
end

##################################################################
# Connect & Disconnect
##################################################################

function connect(server::String = "localhost", port::Int = 9042)
  con = CQLConnection(Base.connect(server, port),
                      IOBuffer(), 1, Dict(), 0);
  sendMessage(con, 0x01, {"CQL_VERSION" => "3.0.0"});
  version, id, opcode, len = readServerMessage(con.socket);
  con.pending = 0;
  @assert version == 0x82
  @assert opcode  == 0x02
  @async handleServerMessages(con);
  con 
end

function disconnect(con::CQLConnection)
  while 0 < con.pending
    yield();
  end
  close(con.socket);
  con.socket  = Base.TcpSocket();
  con.buffer  = IOBuffer();
  con.msg_id  = 1;
  con.replies = Dict();
  con.pending = 0;
  con 
end

##################################################################
# Handle Server Messages
##################################################################

function readServerMessage(socket::Base.TcpSocket)
  version = read(socket, Uint8);
  flags   = read(socket, Uint8);
  id      = read(socket, Uint8);
  opcode  = read(socket, Uint8);
  len     = ntoh(read(socket, Uint32));
  (version, id, opcode, len)
end
  
function handleServerMessage(con::CQLConnection)
  version, id, opcode, len = readServerMessage(con.socket);
  if id > 0 then
    put!(pop!(con.replies, id), 
         (opcode, readbytes(con.socket, len)));
  elseif opcode == 0x00 then
    println("ERROR: ", bytestring(readbytes(con.socket, len)));
  else
    for i in 1:len
      read(con.socket, Uint8);
    end
  end
  con.pending -= 1;
  nothing
end

function handleServerMessages(con::CQLConnection)
  while !eof(con.socket)
    try
      handleServerMessage(con);
    catch err
      isa(err, EOFError) ? nothing : throw(err);
    end
  end
  nothing
end

##################################################################
### Decoding #####################################################
##################################################################

function decodeString(s::IO)
  strlen = ntoh(read(s, Int16));
  bytestring(readbytes(s, strlen));
end

function decodeList(s::IO, val_type)
  nrOfElements = int(ntoh(read(s, Uint16))); 
  ar = Array(Any, nrOfElements);
  for i in 1:nrOfElements
    nrOfBytes = ntoh(read(s, Int16)); 
    ar[i] = decodeValue(s, nrOfBytes, val_type);
  end
  ar
end

function decodeDict(s::IO, key_type, val_type)
  d = Dict();
  nrOfElements = int(ntoh(read(s, Uint16))); 
  for i in 1:nrOfElements
    nrOfBytes = ntoh(read(s, Int16)); 
    key = decodeValue(s, nrOfBytes, key_type);
    nrOfBytes = ntoh(read(s, Int16)); 
    val = decodeValue(s, nrOfBytes, val_type);
    d[key] = val;
  end
  d
end

function decodeValue(s::IO, nrOfBytes::Integer, 
                     type_kind::Uint16, 
                     val_type = nothing, 
                     key_type = nothing)
  nrOfBytes < 0     ? nothing :
  type_kind == 0x02 ? ntoh(read(s, Uint64)) : 
  type_kind == 0x09 ? int(ntoh(read(s, Uint32))) :
  type_kind == 0x0B ? ("Timestamp", ntoh(read(s, Uint64))) :
  type_kind == 0x0C ? ("UUID", ntoh(read(s, Uint128))) :
  type_kind == 0x0D ? bytestring(readbytes(s, nrOfBytes)) :
  type_kind == 0x20 ? decodeList(s, val_type) :
  type_kind == 0x21 ? decodeDict(s, key_type, val_type) :
  type_kind == 0x22 ? Set(decodeList(s, val_type)) :
            ("*NYI*", type_kind, readbytes(s, nrOfBytes))
end

function decodeResultRows(s::IOBuffer)
  flags = ntoh(read(s, Uint32)); 
  colcnt = ntoh(read(s, Uint32)); 
  global_tables_spec = (flags & 0x0001) != 0;
  
  if global_tables_spec then
    global_ksname = decodeString(s);
    global_tablename = decodeString(s);
  end
  if (flags & 0x0002) != 0 then
    println("d >> ");
  end
  if (flags & 0x0004) != 0 then
    println("e >> ");
  end
  col_type = Array(Uint16, colcnt);
  col_sub_type = Array(Uint16, colcnt);
  col_key_type = Array(Uint16, colcnt);
  for col in 1:colcnt
    if global_tables_spec then
      ksname = global_ksname;
      tablename = global_tablename;
    else
      ksname = decodeString(s);
      tablename = decodeString(s);
    end
    name = decodeString(s);
    type_kind = ntoh(read(s, Uint16)); 
    col_type[col] = type_kind;
    if type_kind == 0x20 || type_kind == 0x22 then
      option_id = ntoh(read(s, Uint16)); 
      col_sub_type[col] = option_id;
    elseif type_kind == 0x21 then
      key_type = ntoh(read(s, Uint16)); 
      value_type = ntoh(read(s, Uint16)); 
      col_key_type[col] = key_type;
      col_sub_type[col] = value_type;
    end
    #println(col, " :: $ksname.$tablename.$name : $type_kind")
  end

  row_count = ntoh(read(s, Uint32)); 
  values = Array(Any, row_count);
  for row in 1:row_count
    values[row] = row_value = Array(Any, colcnt);
    for col in 1:colcnt
      nrOfBytes = ntoh(read(s, Int32)); 
      row_value[col] = decodeValue(s, nrOfBytes, col_type[col], 
                          col_sub_type[col], col_key_type[col]);
    end
  end
  values
end

function decodeResultMessage(buffer::Array{Uint8})
  s = IOBuffer(buffer);
  kind = ntoh(read(s, Uint32)); 
  kind == 0x01 ? {"void"} :
  kind == 0x02 ? decodeResultRows(s) :
  kind == 0x03 ? {"set keyspace", decodeString(s)} :
  kind == 0x04 ? {"prepared"} :
  kind == 0x05 ? {"schema change", decodeString(s), 
                  decodeString(s), decodeString(s)} :
       {"???"}
end

function decodeMessage(opcode::Uint8, buffer::Array{Uint8})
  opcode == 0x08 ? decodeResultMessage(buffer) :
  opcode == 0x00 ? ( errmsg = bytestring(buffer);
                     println("ERROR: ", errmsg);
                     {"ERROR", errmsg} ) :
         {opcode, buffer};
end

##################################################################
### Encoding #####################################################
##################################################################

function cql_encode_string(buf::IOBuffer, str::String)
  encStr = bytestring(is_valid_utf8(str) ? str : utf8(str));
  write(buf, hton(uint16(sizeof(encStr))));
  write(buf, encStr);
  nothing
end

function cql_encode_long_string(buf::IOBuffer, str::String)
  encStr = bytestring(is_valid_utf8(str) ? str : utf8(str));
  write(buf, hton(uint32(sizeof(encStr))));
  write(buf, encStr);
  nothing
end

##################################################################
# Encoding
##################################################################

function cql_encode(buf::IOBuffer, dict::Dict)
  write(buf, hton(uint16(length(dict))));
  for (k,v) in dict
    cql_encode_string(buf, k);
    cql_encode_string(buf, v);
  end
  nothing
end

function cql_encode(buf::IOBuffer, query::String)
  cql_encode_long_string(buf, query);
  write(buf, 0x00); 
  write(buf, 0x04); 
  write(buf, 0x00); 
  nothing
end

##################################################################
# Sending Message to the server
##################################################################

function sendMessageBody(con::CQLConnection, msg)
  buf = con.buffer;
  truncate(buf, 0);
  cql_encode(buf, msg);
  write(con.socket, hton(uint32(buf.size)));
  write(con.socket, takebuf_array(buf));
end

function sendMessage(con::CQLConnection, kind::Uint8,
                     msg, id :: Uint8 = 0x00)
  con.pending += 1;
  write(con.socket, 0x02);
  write(con.socket, 0x00);
  write(con.socket, id);
  write(con.socket, kind);
  sendMessageBody(con, msg);

  flush(con.socket);
  yield();
  nothing
end

function nextReplyID(con::CQLConnection)
  id :: Uint8 = con.msg_id;
  con.msg_id = 1 + (id + 1) % 99;
  while haskey(con.replies, id)
    yield();
  end
  reply = RemoteRef();
  con.replies[id] = reply;
  (id, reply)
end

##################################################################
# Queries
##################################################################
 
function query(con::CQLConnection, msg::String)
  sync(con);
  getResult(asyncQuery(con, msg))
end

function command(con::CQLConnection, msg::String)
  sync(con);
  asyncCommand(con, msg)
  sync(con);
  nothing
end

function asyncQuery(con::CQLConnection, msg::String)
  id, reply = nextReplyID(con);
  sendMessage(con, 0x07, msg, id);
  reply
end

function asyncCommand(con::CQLConnection, msg::String)
  sendMessage(con, 0x07, msg);
  nothing
end

function getResult(reply::RemoteRef)
  decodeMessage(take!(reply) ...)
end

function sync(con::CQLConnection)
  while 0 < con.pending
    yield();
  end
end

##################################################################

end

