import socket  # noqa: F401
import threading
import utils.parser as parser
import datetime
import argparse

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    argP = argparse.ArgumentParser()
    argP.add_argument("--dir")
    argP.add_argument("--dbfilename")
    argP.add_argument("--port")
    argP.add_argument("--replicaof")
    args = argP.parse_args()

    data = {}
    if args.dir:
        data["dir"] = args.dir
        data["dbfilename"] = args.dbfilename
        parser.read_key_val_from_db(data["dir"], data["dbfilename"], data)
    
    port = 6379

    if args.port:
        port = int(args.port)
    if args.replicaof:
        data["role"] = "slave"
        masterConf = args.replicaof.split()
        data["master_host"] = masterConf[0]
        data["master_port"] = masterConf[1]
    else:
        data["role"] = "master"
        data["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        data["master_repl_offset"] = "0"

    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    if data["role"] == "slave":
        sock = socket.create_connection((data["master_host"], int(data["master_port"])))
        sock.send(parser.encode(["PING"]))
        while True:
            req = parser.parse(sock.recv(1024))
            print(req)
            if req == "PONG":
                sock.send(parser.encode(["REPLCONF", "listening-port", str(port)]))
                sock.send(parser.encode(["REPLCONF", "capa", "psync2"]))
                sock.recv(1024)
                sock.send(parser.encode(["PSYNC", "?", "-1"]))
                sock.recv(1024)
                break

    while True:
        conn, address = server_socket.accept() # wait for client
        thread = threading.Thread(target=handle_conn,   args=(conn, address, data))
        thread.start()
        

def handle_conn(conn, address, data):
    try:    
        while True:
            req = parser.parse(conn.recv(1024))
            
            if req[0] == "PING":
                resp = parser.encode("PONG")
                conn.send(resp)
            elif req[0] == "ECHO":
                resp = parser.encode(req[1])
                conn.send(resp)
            elif req[0] == "SET":
                if len(req) > 3 and req[3] == "px":
                    data[req[1]] = (req[2], datetime.datetime.now().timestamp()*1000 + int(req[4]))
                else:
                    data[req[1]] = (req[2], -1)
                resp = parser.encode("OK")

                if 'slaves' in data:
                    for slave in data['slaves']:
                        slave.send(parser.encode(req))

                if data['role'] != 'slave':
                    conn.send(resp)
            elif req[0] == "GET":
                ans = None
                if req[1] in data:
                    if(data[req[1]][1] == -1 or data[req[1]][1] > datetime.datetime.now().timestamp()*1000):
                        ans = data[req[1]][0].encode('utf-8')
                    else:
                        data.pop(req[1])
                
                resp = parser.encode(ans)
                conn.send(resp)
            elif req[0] == "CONFIG":
                ans = None
                if req[1] == "GET":
                    ans = [req[2], data[req[2]]]
                resp = parser.encode(ans)
                conn.send(resp)
            elif req[0] == "KEYS":
                ans = None
                if req[1] == "*":
                    ans = parser.read_rdb_key(data['dir'], data['dbfilename'])
                resp = parser.encode(ans)
                conn.send(resp)
            elif req[0] == "INFO":
                ans = ""
                if data['role'] == "slave":
                    ans = "role:slave"
                else:
                    ans = "role:master"
                    ans = ans + " \n " + "master_replid:" + data["master_replid"]
                    ans = ans + " \n " + "master_repl_offset:" + data["master_repl_offset"]
                resp = parser.encode(ans)
                conn.send(resp)
            elif req[0] == "REPLCONF":
                resp = parser.encode("OK")
                conn.send(resp)
            elif req[0] == "PSYNC":
                ans = "FULLRESYNC" + " " + data["master_replid"] + " " + data["master_repl_offset"]
                resp = parser.encode(ans)
                conn.send(resp)

                rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
                rdb_bytes = bytes.fromhex(rdb_hex)
                ans = f"${len(rdb_bytes)}\r\n".encode()
                resp = ans + rdb_bytes

                # store slave connection for replication
                if 'slaves' not in data:
                    data['slaves'] = []
                data['slaves'].append(conn)
                conn.send(resp)
            
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print( message)
        


if __name__ == "__main__":
    main()
