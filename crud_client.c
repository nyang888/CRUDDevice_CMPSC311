////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <crud_driver.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <unistd.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server

// Global variables to store connection info
int socket_fd;
struct sockaddr_in caddr;
int isConnect = 0;

//
// Functions
void my_cruddy_send(CrudRequest req, char *buf);
CrudResponse my_cruddy_receive(CrudResponse res, char *buf);
void extract_crud_request(CrudRequest crud, int *req, int *length);

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
	// Check if already connected
	if (isConnect != 1){
		// Prepare for connections
		socket_fd = socket(PF_INET, SOCK_STREAM, 0);
		caddr.sin_family = AF_INET;
		caddr.sin_port = htons(CRUD_DEFAULT_PORT);
		inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr);

		// Connect to server 
		connect(socket_fd, (const struct sockaddr*)&caddr, sizeof(struct sockaddr));
		isConnect = 1;
	}

	// Send request to server
	my_cruddy_send(op, (char*)buf);
	CrudResponse read = my_cruddy_receive(op, (char*)buf);

	return read;
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: my_cruddy_send
// Description	: This checks what kind of CrudRequest is used then sends the
// 		  the request to the server.
//
// Inputs	: req - the CrudRequest
// 		  buf - the block to read/write from
// Outputs	: none

void my_cruddy_send(CrudRequest req, char *buf){
	// Local variables for request info
	int request;
	int length;

	// Extract req to see what the actual request is
	extract_crud_request(req, &request, &length);

	// Translate the request to network byte order
	CrudRequest netReq = htonll64(req);

	// Send the request
	write(socket_fd, &netReq, sizeof(netReq));

	// Compare request to current request type and send buffer if necessary
	if (request == CRUD_CREATE){
		// Send the buffer, use loop to ensure entire buf is sent
		int bufCount = 0;
		do {
			int retBuf = write(socket_fd, &buf[bufCount], length-bufCount);
			bufCount = bufCount + retBuf;
		} while (bufCount != length);
	} else if (request == CRUD_UPDATE){
		// Send the buffer, use loop to ensure entire buf is sent
		int bufCount = 0;
		do {
			int retBuf = write(socket_fd, &buf[bufCount], length-bufCount);
			bufCount = bufCount + retBuf;
		} while(bufCount != length);
	}
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: my_cruddy_receive
// Description	: This checks what kind of CrudRequest is used then receives the
// 		  the corresponding information
//
// Inputs	: req - the CrudRequest
// 		  buf - the block to read/write from
// Outputs	: returns the CrudResponse that was sent back

CrudResponse my_cruddy_receive(CrudRequest req, char *buf){
	// Local variables to store request info
	int request;
	int length;

	// Extract req to see what the actual request is
	extract_crud_request(req, &request, &length);

	// Create variables to store the current response
	CrudResponse tempResp;
	CrudResponse netResp;

	// Read the resonse 
	read(socket_fd, &tempResp, sizeof(tempResp));

	// Convert CrudResponse to local byte order
	netResp = ntohll64(tempResp);

	// Compare request to current request type and send buffer if necessary
	if (request == CRUD_READ){
		int bufCount = 0;
		do {
			int retBuf = read(socket_fd, &buf[bufCount], length-bufCount);
			bufCount = bufCount + retBuf;
		} while(bufCount != length);
	} else if (request == CRUD_CLOSE){
		close(socket_fd);
		socket_fd = -1;
		isConnect = 0;
	}

	return netResp;
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: extract_crud_request
// Description	: extracts the request sent to crud_client operation
//
//  Inputs	: CrudRequest
//  		  *req - pointer to request type value
//  		  *length = pointer to length value
//  Outputs	: none

void extract_crud_request( CrudRequest crud, int *req, int *length ) {
	*req =(int) (int64_t) (crud & (((int64_t)1<<32)-1))>>28;
	*length = (int)(int64_t) (crud & ((1<<28)-1))>>4;
}
