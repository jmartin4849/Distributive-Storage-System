# Distributive-Storage-System
Distributive Storage System to implement a scalable key-value storage system capable of responding to high volume client storage requests seamlessly.

Created by Johnathon Martin and Lunjun Zhang
# Milestone 1
The first milestone for this project was implementing a client server system capable of proccessing put, get and remove requests.

Storage was written to disk in the form of a JSON file for easy lookup and removal of key-value pairs.  Writing the values to disks allowed the server to maintain storage upon server shutdown/restart.  The Server-Client cummunication was implemented via a socket programming.  A simple parser was implemented client-side inorder to user to interact with the client interface.

The storage server is implemented using multithreading and Read Write locks in order to eliminate race conditions while keeping read times fast.

Here is a simple diagram demenstrating how the client-server communication works at a high level.

![Alt text](Images/m1-1.png)

# Milestone 2
THis is Milestone 2
# Milestone 3
This is Milestone 3
# Milestone 4
This is Milestone 4
