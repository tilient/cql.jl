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
    kind   = ntoh(read(con.socket, Int32));
    strlen = ntoh(read(con.socket, Int16));
    println("ERROR [$kind] : ", 
            bytestring(readbytes(con.socket, strlen)));
  else
    for _ in 1:len
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
### UUID #########################################################
##################################################################

type UUID
  val::Uint128
end

function Base.print(io::IO, uuid::UUID)                          
  h = hex(uuid.val,32);
  print(io, "\"");
  print(io, h[1:7]);
  print(io, "-");
  print(io, h[8:11]);
  print(io, "-");
  print(io, h[12:15]);
  print(io, "-");
  print(io, h[16:19]);
  print(io, "-");
  print(io, h[20:32]);
  print(io, "\"");
end

Base.show (io::IO, uuid::UUID) = print(io, uuid);

##################################################################
### Timestamp ####################################################
##################################################################

type Timestamp 
  milliseconds::Uint64
end

function Base.print(io::IO, t::Timestamp)
  print(io, "\"");
  print(io, strftime(t.milliseconds / 1000));
  print(io, "\"");
end

Base.show (io::IO, t::Timestamp) = print(io, t);

##################################################################
### Decoding #####################################################
##################################################################

function decodeString(s::IO)
  strlen = ntoh(read(s, Int16));
  bytestring(readbytes(s, strlen));
end

function decodeResultSubColumn(s::IO, typ) 
  nrOfBytes = ntoh(read(s, Int16));
  decodeValue(s, nrOfBytes, (typ, nothing, nothing))
end

function decodeList(s::IO, typ)
  nrOfElements = ntoh(read(s, Uint16)); 
  ar = Array(Any, nrOfElements);
  for ix in 1:nrOfElements
    ar[ix] = decodeResultSubColumn(s, typ);
  end
  ar
end

function decodeMap(s::IO, val_type)
  Set(decodeList(s, val_type))
end

function decodeDict(s::IO, key_type, val_type)
  d = Dict();
  nrOfElements = int(ntoh(read(s, Uint16))); 
  for i in 1:nrOfElements
    key = decodeResultSubColumn(s, key_type);
    val = decodeResultSubColumn(s, val_type);
    d[key] = val;
  end
  d
end

function decodeValue(s::IO, nrOfBytes::Integer, types) 
  type_kind, val_type, key_type = types;
  nrOfBytes < 0     ? nothing :
  type_kind == 0x02 ? ntoh(read(s, Uint64)) : 
  type_kind == 0x09 ? int(ntoh(read(s, Uint32))) :
  type_kind == 0x0B ? (Timestamp(ntoh(read(s, Uint64)))) :
  type_kind == 0x0C ? (UUID(ntoh(read(s, Uint128)))) :
  type_kind == 0x0D ? bytestring(readbytes(s, nrOfBytes)) :
  type_kind == 0x20 ? decodeList(s, val_type) :
  type_kind == 0x21 ? decodeDict(s, key_type, val_type) :
  type_kind == 0x22 ? decodeMap(s, val_type) :
            ("*NYI*", type_kind, readbytes(s, nrOfBytes))
end

function decodeResultRowTypes(s::IOBuffer)
  flags   = ntoh(read(s, Uint32)); 
  colcnt  = ntoh(read(s, Uint32)); 
  gl_spec = (flags & 0x0001) != 0;
  
  if gl_spec then
    gl_ksname = decodeString(s);
    gl_tablename = decodeString(s);
  end
  if (flags & 0x0002) != 0 then
    println("d >> ");
  end
  if (flags & 0x0004) != 0 then
    println("e >> ");
  end
  types = Array((Int16,Any,Any), colcnt);
  for col in 1:colcnt
    ksname     = gl_spec ? gl_ksname : decodeString(s);
    tablename  = gl_spec ? gl_tablename : decodeString(s);
    name       = decodeString(s);
    kind       = ntoh(read(s, Uint16)); 
    key_type   = kind == 0x21 ?
                   ntoh(read(s, Uint16)) : nothing;
    sub_type   = kind in {0x20, 0x21, 0x22} ?
                   ntoh(read(s, Uint16)) : nothing; 
    types[col] = (kind, sub_type, key_type);
  end
  types
end

function decodeResultColumn(s::IO, typ) 
  nrOfBytes = ntoh(read(s, Int32));
  decodeValue(s, nrOfBytes, typ)
end

function decodeResultRows(s::IOBuffer)
  types = decodeResultRowTypes(s);
  nrOfRows = ntoh(read(s, Uint32));
  ar = Array(Any, nrOfRows)
  for row = 1:nrOfRows
    ar[row] = map(typ -> decodeResultColumn(s, typ), types)
  end
  ar
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

function decodeErrorMessage(buffer::Array{Uint8})
  s = IOBuffer(buffer);
  kind = ntoh(read(s, Uint32)); 
  errmsg = decodeString(s);
  println("ERROR [$kind]: ", errmsg);
  {"ERROR", kind, errmsg} 
end

function decodeMessage(opcode::Uint8, buffer::Array{Uint8})
  opcode == 0x08 ? decodeResultMessage(buffer) :
  opcode == 0x00 ? decodeErrorMessage(buffer) :
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

