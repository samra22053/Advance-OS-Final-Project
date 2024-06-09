import socket
import threading
import os

class ChunkServer:
    def __init__(self, host, port, storage_dir):
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)  # Ensure the storage directory exists

    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        try:
            data = conn.recv(1024).decode()  # Receive command from the client
            command_parts = data.split()
            if len(command_parts) < 2:
                raise ValueError("Invalid command format")  # Check for valid command format

            command = command_parts[0]
            chunk_id = command_parts[1]

            if command == "STORE":
                self.store_chunk(conn, chunk_id)  # Handle storing the chunk
            elif command == "RETRIEVE":
                self.retrieve_chunk(conn, chunk_id)  # Handle retrieving the chunk
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()  # Ensure the connection is closed after handling the client

    def store_chunk(self, conn, chunk_id):
        print(f"Storing chunk {chunk_id}")
        chunk_data = conn.recv(1024 * 1024)  # Receive chunk data from the client
        chunk_id = chunk_id.split(':')[0]  # Ensure proper chunk_id formatting
        chunk_path = os.path.join(self.storage_dir, chunk_id)  # Determine path for storing chunk
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)  # Write chunk data to file
        print(f"Stored chunk {chunk_id}")

    def retrieve_chunk(self, conn, chunk_id):
        print(f"Retrieving chunk {chunk_id}")
        chunk_id = chunk_id.split(':')[0]  # Ensure proper chunk_id formatting
        chunk_path = os.path.join(self.storage_dir, chunk_id)  # Determine path to retrieve chunk
        if os.path.exists(chunk_path):
            with open(chunk_path, 'rb') as f:
                conn.sendall(f.read())  # Send chunk data to the client
            print(f"Retrieved chunk {chunk_id}")
        else:
            conn.sendall(b"ERROR: Chunk not found")  # Notify client if chunk is not found
            print(f"Chunk {chunk_id} not found")

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))  # Bind the server to the specified host and port
            s.listen()  # Start listening for incoming connections
            print(f"Chunk server started on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()  # Accept new connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()  # Handle client in a new thread

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python3 chunk_server.py <host> <port> <storage_dir>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    storage_dir = sys.argv[3]

    server = ChunkServer(host, port, storage_dir)  # Initialize the chunk server
    server.start()  # Start the chunk server
