import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, address = server_socket.accept() # wait for client

    while True:

        req = conn.recv(1024).decode("utf-8")
        print(req)
        if req == "*1\r\n$4\r\nPING\r\n":
            conn.send("+PONG\r\n".encode("utf-8"))



if __name__ == "__main__":
    main()
