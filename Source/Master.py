import socket
import threading
import time

class MasterServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.files = {}  # Dictionary to store filenames and their associated chunk IDs
        self.chunk_servers = ["192.168.231.129:50054", "192.168.231.129:50052"]  # Addresses of chunk servers

    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        try:
            data = conn.recv(1024).decode()  # Receive initial command from the client
            parts = data.split()
            command = parts[0]  # Extract the command (CREATE, LIST, GET_CHUNKS, DOWNLOAD)

            if command == "CREATE":
                filename = parts[1]
                size = int(parts[2])
                file_data = conn.recv(size)  # Receive the file data from the client
                self.create_file(conn, filename, file_data)
            elif command == "LIST":
                self.list_files(conn)
            elif command == "GET_CHUNKS":
                filename = parts[1]
                self.get_chunks(conn, filename)
            elif command == "DOWNLOAD":
                filename = parts[1]
                self.download_file(conn, filename)
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()  # Ensure the connection is closed after handling the client

    def create_file(self, conn, filename, data):
        print(f"Creating file {filename} with data of size {len(data)} bytes")
        
        # Adjust the chunk size
        chunk_size = (len(data) + len(self.chunk_servers) - 1) // len(self.chunk_servers)
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]  # Split data into chunks

        self.files[filename] = []
        for i, chunk in enumerate(chunks):
            chunk_id = f"{filename}_chunk{i}"
            self.files[filename].append(chunk_id)  # Store chunk IDs
            for server in self.chunk_servers:
                self.send_chunk_to_server(server, chunk_id, chunk)  # Send chunks to chunk servers

        conn.sendall(b"File created successfully")  # Notify client that file creation was successful

    def list_files(self, conn):
        files_list = "\n".join(self.files.keys())  # Create a list of filenames
        conn.sendall(files_list.encode())  # Send the list to the client

    def get_chunks(self, conn, filename):
        chunks_list = " ".join(self.files.get(filename, []))  # Get list of chunk IDs for the file
        conn.sendall(chunks_list.encode())  # Send chunk IDs to the client

    def send_chunk_to_server(self, server, chunk_id, chunk):
        host, port = server.split(':')
        # Create a new thread to send the chunk to the server
        threading.Thread(target=self.send_chunk_to_server_thread, args=(host, int(port), chunk_id, chunk)).start()

    def send_chunk_to_server_thread(self, host, port, chunk_id, chunk):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                full_message = f"STORE {chunk_id} {len(chunk)}".encode()
                s.sendall(full_message)  # Send command to store the chunk
                time.sleep(0.1)  # Add delay to ensure data is sent properly
                s.sendall(chunk)  # Send the chunk data
                print(f"Sent chunk {chunk_id} to {host}:{port}")
        except Exception as e:
            print(f"Error sending chunk {chunk_id} to {host}:{port}: {e}")

    def download_file(self, conn, filename):
        if filename not in self.files:
            conn.sendall(b"ERROR: File not found")
            return

        combined_data = b""
        for chunk_id in self.files[filename]:
            chunk_data = self.retrieve_chunk(chunk_id)
            if chunk_data is not None:
                combined_data += chunk_data  # Combine all chunks to form the complete file

        conn.sendall(combined_data)  # Send the combined file data to the client

    def retrieve_chunk(self, chunk_id):
        for server in self.chunk_servers:
            host, port = server.split(':')
            chunk_data = self.retrieve_chunk_from_server(host, int(port), chunk_id)
            if chunk_data is not None:
                return chunk_data  # Return chunk data if retrieved successfully
        return None

    def retrieve_chunk_from_server(self, host, port, chunk_id):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(f"RETRIEVE {chunk_id}".encode())  # Send command to retrieve the chunk
                chunk_data = s.recv(1024 * 1024)  # Adjust buffer size as needed
                if chunk_data.startswith(b"ERROR"):
                    return None  # Return None if an error message is received
                return chunk_data
        except Exception as e:
            print(f"Error retrieving chunk {chunk_id} from {host}:{port}: {e}")
            return None

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()  # Start listening for incoming connections
            print("Master server started, waiting for connections...")
            while True:
                conn, addr = s.accept()  # Accept new connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()  # Handle client in a new thread

if __name__ == "__main__":
    master = MasterServer('0.0.0.0', 50050)  # Initialize the master server
    master.start()  # Start the master server

