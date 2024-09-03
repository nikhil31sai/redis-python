import socket  # noqa: F401
import threading
import utils.parser as parser

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:

        conn, address = server_socket.accept() # wait for client

        thread = threading.Thread(target=handle_conn,   args=(conn, address))
        thread.start()
        

def handle_conn(conn, address):
    while True:
        req = parser.parse(conn.recv(1024))
        
        if req[0] == "PING":
            resp = parser.encode("PONG")
        elif req[0] == "ECHO":
            resp = parser.encode(req[1])
        conn.send(resp)
        


if __name__ == "__main__":
    main()
