File Distribution System

Abstract:

This project aims to develop a file distribution system using Python and Ubuntu. The system ensures redundancy and fault tolerance by saving each file in multiple chunks and distributing copies of each chunk across the network. Specifically, the system guarantees that each file is divided into at least two chunks, and each chunk has multiple distributed copies to ensure data integrity and availability.

Overview:

This project focuses on creating a simple file distribution system with the following key features:

1. Each file is saved in more than one chunk, with a minimum of two chunks.
2. Each chunk of a file has multiple distributed copies across the network.

The idea is to design the DFS system. For stage 3/3:

Idea: A Distributed File System (DFS) that will be implemented on the master server. The client and master server will communicate in order to upload/download files. The master server then saves the data and distributes it in chunks to the chunk servers, from where it will be retrieved in case of download. Communication is an important part of this project that will allow the server to run on different machines.

Completed Tasks:

 Client, master, and chunk servers are implemented. 
 The Chunk server is open for communication. 
 The Master server is able to allocate chunks to the chunk servers.
 GUI is created and Error handling has been done (Deleting file)

[How to run the files:]

On the 1st OS (Ubuntu):

Open 3 terminals:

 On the 1st terminal, to run the master code, type: `sudo python3 Master.py`
 On the 2nd terminal, to run the chunk server, type: `python3 Chunk_server.py 192.168.231.129 50052 output1 &` and repeat this for the    2nd chunk server as well.

On the 2nd OS (Ubuntu):

Open 1 terminal:

 On the terminal, to run the client code, type: `sudo python3 Client.py`

General Notes:
- One terminal is for the client.py, and two terminals are for the chunk servers with different port numbers on localhost.
- Run the commands as described in the bin file.

After running the file, the input file to be uploaded will be successfully uploaded to the chunk servers via the master server. The data file system will create a file, upload a file, and communicate with the chunk servers to distribute the chunks. The files in the DFS list can be viewed in the GUI of the client and can be selected for download. Refreshing the list will delete all the files uploaded via DFS. The number of bytes transferred can be viewed as well. The size of the created chunks is also part of the functions.

Contribution:

 Abdullah Shahani (MSCS22058):

The DFS system on the master end includes creating files, communication between chunks and master, and uploading and retrieving files from and to the chunk servers. We both worked on the errors that halted the successful running of the code. We addressed the communication issue; the path for uploading and downloading was not working properly. To counter this issue, chunks need to be uploaded successfully. In order to do so, chunks are now divided into two almost equal sizes of the total data. This approach was tried because of data loss during chunk creation. However, now I think I can resolve it for infinite chunks as well, but I stuck to the basics as there are only two chunk servers, and Sir told us that chunks can be two as well.The connection between the chunk server and the master was in the initial stage. Initially, a command-line prompt was introduced to understand and check if the functions were running properly.

 Saima Ali (MSCS22053):

Deliverable 1, which is creating the functions for the proper functioning of the DFS system, has been proposed by Saima Ali. The work has been successfully done.  I further worked on the issue where the file size differed on the client side and the master side. The master was receiving fewer bytes than expected. I worked really hard, tried every method to resolve the issue, and finally resolved it by adding a delay and some debugging features on the client side. I created a GUI interface for the client. I interfaced the GUI with the client's functions and did testing and debugging of the code to eradicate errors.


Note:

The project is completed. For future reference, the chunk sizes and number can be enhanced. Direct communication between the chunks and clients can be implemented.

GitHub Path: https://github.com/Abdullah512344/Advance-OS-Project-DFS-.git
Other Path: https://github.com/samra22053/Advance-OS-Final-Project.git

