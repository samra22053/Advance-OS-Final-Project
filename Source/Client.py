import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import socket
import os
import time

class DFSClientGUI:
    def __init__(self, master):
        self.master = master
        master.title("Distributed File System Client")

        # Get screen width and height
        screen_width = master.winfo_screenwidth()
        screen_height = master.winfo_screenheight()

        # Set window size to more than half the screen
        window_width = int(screen_width * 0.7)
        window_height = int(screen_height * 0.7)

        # Center the window on the screen
        position_right = int(screen_width / 2 - window_width / 2)
        position_down = int(screen_height / 2 - window_height / 2)
        master.geometry(f"{window_width}x{window_height}+{position_right}+{position_down}")

        # Define the style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('TFrame', background='#3b5998')
        self.style.configure('TButton', background='#8b9dc3', foreground='white', font=('Helvetica', 12, 'bold'))
        self.style.configure('TLabel', background='#3b5998', foreground='white')

        # Rainbow colors for buttons
        self.button_colors = ["#ff6666", "#ffcc66", "#66ff66", "#66ccff", "#cc66ff", "#ff66cc"]

        # Main frame
        self.main_frame = ttk.Frame(master, padding="10 10 10 10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # Title label
        self.label = ttk.Label(self.main_frame, text="DFS Client", font=('Helvetica', 24, 'bold'))
        self.label.pack(pady=20)

        # Upload button
        self.upload_button = tk.Button(self.main_frame, text="Upload File", command=self.upload_file, bg=self.button_colors[0], fg='white', font=('Helvetica', 12, 'bold'))
        self.upload_button.pack(pady=10)

        # Download button
        self.download_button = tk.Button(self.main_frame, text="Download File", command=self.download_file, bg=self.button_colors[1], fg='white', font=('Helvetica', 12, 'bold'))
        self.download_button.pack(pady=10)

        # Refresh button
        self.refresh_button = tk.Button(self.main_frame, text="Refresh File List", command=self.refresh_file_list, bg=self.button_colors[2], fg='white', font=('Helvetica', 12, 'bold'))
        self.refresh_button.pack(pady=10)

        # File list label
        self.file_list_label = ttk.Label(self.main_frame, text="Files in DFS:")
        self.file_list_label.pack(pady=10)

        # File listbox
        self.file_listbox = tk.Listbox(self.main_frame)
        self.file_listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # IP and port of the master server
        self.master_address = ('192.168.231.129', 50050)

        # Start by refreshing the file list
        self.refresh_file_list()

    def upload_file(self):
        # Prompt user to select a file for uploading
        filepath = filedialog.askopenfilename()
        if filepath:
            # Read the file data
            filename = os.path.basename(filepath)
            with open(filepath, 'rb') as f:
                file_data = f.read()
            file_size = len(file_data)
            print(f"File size reported during creation: {file_size}")

            # Send CREATE command to the master server
            message = f"CREATE {filename} {file_size}"
            response = self.send_to_master(message, file_data)
            messagebox.showinfo("Upload", response.decode())
            self.refresh_file_list()

    def download_file(self):
        # Get selected file from the listbox
        selected_files = self.file_listbox.curselection()
        if selected_files:
            filename = self.file_listbox.get(selected_files[0])
            message = f"DOWNLOAD {filename}"
            file_data = self.send_to_master(message)

            # Prompt user to select a location to save the downloaded file
            save_path = filedialog.asksaveasfilename(defaultextension=".txt", initialfile=filename)
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(file_data)
                messagebox.showinfo("Download", f"{filename} downloaded successfully.")
        else:
            messagebox.showwarning("Download", "Please select a file to download.")

    def refresh_file_list(self):
        # Clear the listbox
        self.file_listbox.delete(0, tk.END)
        
        # Send LIST command to the master server
        message = "LIST"
        response = self.send_to_master(message)
        file_list = response.decode().split("\n")
        for file in file_list:
            if file:
                self.file_listbox.insert(tk.END, file)

    def send_to_master(self, message, data=b''):
        # Send message and optional data to the master server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.master_address)
            s.sendall(message.encode())
            if data:
                time.sleep(5)  # Ensure the master server is ready to receive the data
                s.sendall(data)

            response = s.recv(1024 * 1024)  # Adjust buffer size as needed
            return response

if __name__ == "__main__":
    # Initialize and start the GUI
    root = tk.Tk()
    client = DFSClientGUI(root)
    root.mainloop()

