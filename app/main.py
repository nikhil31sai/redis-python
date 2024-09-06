import socket  # noqa: F401
import threading
import utils.parser as parser
import datetime
import sys

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    print(sys.argv[0])
    data = {}
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:

        conn, address = server_socket.accept() # wait for client

        thread = threading.Thread(target=handle_conn,   args=(conn, address, data))
        thread.start()
        

def handle_conn(conn, address, data):
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
        conn.send(resp)
        


if __name__ == "__main__":
    main()
