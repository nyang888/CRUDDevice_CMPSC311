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
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server

// Global variables for deconstructing CrudRequest for use in extract_crud_response
int32_t objectID;
int request;
int32_t length;
int flag;
int result;

// Global variables to store connection info
int socket_fd;
struct sockaddr_in caddr;

//
// Functions
int my_cruddy_send(CrudRequest req, void *buf);
CrudResponse my_cruddy_receive(CrudResponse res, void *buf);
void extract_crud_request(CrudRequest crud);

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
/*	int socket_fd;
	struct sockaddr_in caddr;

	// Prepare for connections
	socket_fd = socket(PF_INET, SOCK_STREAM, 0);
	caddr.sin_family = AF_INET;
	caddr.sin_port = htons(CRUD_DEFAULT_PORT);
	inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr);

	// Connect to server
	connect(socket_fd, (const struct sockaddr*)&caddr, sizeof(struct sockaddr));
*/
	// Send request to server
	int send = my_cruddy_send(op, buf);
	CrudResponse read = my_cruddy_receive(op, buf);

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
// Outputs	: a number representing success or failure

int my_cruddy_send(CrudRequest req, void *buf){
	// Extract req to see what the actual request is
	extract_crud_request(req);

	// Translate the request to network byte order
	int64_t netReq = htonll64((int64_t)req);

	// Prepare for connections
	socket_fd = socket(PF_INET, SOCK_STREAM, 0);
	caddr.sin_family = AF_INET;
	caddr.sin_port = htons(CRUD_DEFAULT_PORT);
	inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr);

	// Check if you are currently connected
	// if(getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, 0, sizeof(int)) == 0){
	connect(socket_fd, (const struct sockaddr*)&caddr, sizeof(struct sockaddr));
	//}

	// Send the request
	int count = 0;
	while(count < 64){
		int returned = write(socket_fd, &netReq, sizeof(netReq));

		// Make sure write succeeded
		if (returned < 0){
			printf("Request send fail");
			return -1;
		}

		count = count + returned;
	}

	// Compare request to current request type and send buffer if necessary
	if (request == CRUD_INIT){
		return (CrudResponse)netReq;
	} else if (request == CRUD_FORMAT){
		return (CrudResponse)netReq;
	} else if (request == CRUD_CREATE){
		int bufCount = 0;
		while(bufCount < length) {
			int retBuf = write(socket_fd, buf+bufCount, sizeof(length-bufCount));

			// Make sure write success
			if (retBuf < 0){
				printf("CRUD_CREATE buffer send fail");
				return -1;
			}

			count = count + retBuf;
		}
		return (CrudResponse)netReq;
	} else if (request == CRUD_READ){
		return (CrudResponse)netReq;
	} else if (request == CRUD_UPDATE){
		int bufCount = 0;
		while(bufCount < length) {
			int retBuf = write(socket_fd, buf+bufCount, sizeof(netReq-bufCount));

			// Make sure write success
			if (retBuf < 0){
				printf("CRUD_UPDATE buffer send fail");
				return -1;
			}

			count = count + retBuf;
		}
		return (CrudResponse)netReq;
	} else if (request == CRUD_DELETE){
		return (CrudResponse)netReq;
	} else if (request == CRUD_CLOSE){
		return (CrudResponse)netReq;
	}

	return -1;
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: my_cruddy_receive
// Description	: This checks what kind of CrudRequest is used then receives the
// 		  the corresponding information
//
// Inputs	: req - the CrudRequest
// 		  buf - the block to read/write from
// Outputs	: a number representing success or failure

CrudResponse my_cruddy_receive(CrudRequest req, void *buf){
	// Extract req to see what the actual request is
	extract_crud_request(req);

	// Check if you are currently connected

	// Create variables to store the current response
	int64_t netResp;

	// Read the resonse 
	int count = 0;
	while(count < 64){
		int returned = read(socket_fd, &netResp, sizeof(netResp));

		// Make sure write succeeded
		if (returned < 0){
			printf("Request read fail");
			return -1;
		}

		count = count + returned;
	}

	// Compare request to current request type and send buffer if necessary
	if (request == CRUD_INIT){
		return 0;
	} else if (request == CRUD_FORMAT){
		return 0;
	} else if (request == CRUD_CREATE){
		return 0;
	} else if (request == CRUD_READ){
		int bufCount = 0;
		while(bufCount < length) {
			int retBuf = read(socket_fd, buf+bufCount, sizeof(length-bufCount));

			// Make sure write success
			if (retBuf < 0){
				printf("CRUD_READ buffer read fail");
				return -1;
			}

			count = count + retBuf;
		}
		return 0;
	} else if (request == CRUD_UPDATE){
		return 0;
	} else if (request == CRUD_DELETE){
		return 0;
	} else if (request == CRUD_CLOSE){
		return 0;
	}

	return -1;
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: extract_crud_request
// Description	: extracts the request sent to crud_client operation
//
//  Inputs	: CrudRequest
//  Outputs	: none

void extract_crud_request( CrudRequest crud ) {
	objectID = (int64_t) crud>>32;
	request = (int64_t) (crud & (((int64_t)1<<32)-1))>>28;
	length = (int64_t) (crud & ((1<<28)-1))>>4;
	flag = (int64_t) (crud & ((1<<4)-1))>>1;
	result = (int64_t) crud & 1;
}
