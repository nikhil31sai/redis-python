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
    args = argP.parse_args()

    data = {}
    if args.dir:
        data["dir"] = args.dir
        data["dbfilename"] = args.dbfilename
        parser.read_key_val_from_db(data["dir"], data["dbfilename"], data)
    
    port = 6379

    if args.port:
        port = int(args.port)
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

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
            elif req[0] == "ECHO":
                resp = parser.encode(req[1])
            elif req[0] == "SET":
                if len(req) > 3 and req[3] == "px":
                    data[req[1]] = (req[2], datetime.datetime.now().timestamp()*1000 + int(req[4]))
                else:
                    data[req[1]] = (req[2], -1)
                resp = parser.encode("OK")
            elif req[0] == "GET":
                ans = None
                if req[1] in data:
                    if(data[req[1]][1] == -1 or data[req[1]][1] > datetime.datetime.now().timestamp()*1000):
                        ans = data[req[1]][0].encode('utf-8')
                    else:
                        data.pop(req[1])
                
                resp = parser.encode(ans)
                print("encoding: " + str(ans))
            elif req[0] == "CONFIG":
                ans = None
                if req[1] == "GET":
                    ans = [req[2], data[req[2]]]
                resp = parser.encode(ans)
            elif req[0] == "KEYS":
                ans = None
                if req[1] == "*":
                    ans = parser.read_rdb_key(data['dir'], data['dbfilename'])
                resp = parser.encode(ans)
            conn.send(resp)
            print("Sending response")
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print( message)
        


if __name__ == "__main__":
    main()
