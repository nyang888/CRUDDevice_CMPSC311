////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.h
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Patrick McDaniel
//  Last Modified  : Mon Oct 20 12:38:05 PDT 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

//
// Implementation

// Struct for the generated responses
struct GenResponse {
	int32_t objectId;
	int request;
	int32_t length;
	int flag;
	int succeed;
};

// Function prototypes
CrudRequest create_crud_request(int32_t, int, int32_t, int, int);
void extract_crud_response(CrudResponse, int32_t*, int*, int32_t*, int*, int*);

// Global Variables
int isInit = 0;

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {
	// Check if CRUD is initialized, if not, call CRUD_INIT
	if(isInit == 0) {
		CrudRequest initRequest = create_crud_request( 0, CRUD_INIT, 0, 0, 0 );
		crud_bus_request( initRequest, NULL );
		isInit = 1;
	}
	
	// Create temp buff
	void* tempBuff = calloc(CRUD_MAX_TOTAL_FILES, sizeof(CrudFileAllocationType));

	// Format priority object
	CrudRequest formatRequest = create_crud_request( 0, CRUD_FORMAT, 0, CRUD_NULL_FLAG, 0 );
	CrudResponse formatResponse = crud_bus_request( formatRequest, NULL );

	struct GenResponse response;

	// Check to make sure the CRUD_READ succeeded and extract the values
	extract_crud_response( formatResponse, &response.objectId, &response.request, &response.length,
			&response.flag, &response.succeed );
	if( response.succeed != 0 ) {
		return -1;	
	}

	// Empty table
	int i=0;
	for( i=0; i<CRUD_MAX_TOTAL_FILES; i++ ){
		strcpy(crud_file_table[i].filename,"");
		crud_file_table[i].object_id = 0;
		crud_file_table[i].position = 0;
		crud_file_table[i].length = 0;
		crud_file_table[i].open = 0;
	}

	// Place new table in object store
	CrudRequest createRequest = create_crud_request( 0, CRUD_CREATE, sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES,
			CRUD_PRIORITY_OBJECT, 0 );
	CrudResponse createResponse = crud_bus_request( createRequest, tempBuff );

	// Check to make sure that the CRUD_CREATE succeeded
	extract_crud_response( createResponse, &response.objectId, &response.request, &response.length, 
			&response.flag, &response.succeed );
	if(response.succeed != 0) {
		return -1;
	}

	free(tempBuff);
	tempBuff = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {
	// Check if CRUD is initialized, if not, call CRUD_INIT
	if(isInit == 0) {
		CrudRequest initRequest = create_crud_request( 0, CRUD_INIT, 0, 0, 0 );
		crud_bus_request( initRequest, NULL );
		isInit = 1;
	}

	// Create temp buffer
	void* tempBuff = calloc(CRUD_MAX_TOTAL_FILES, sizeof(CrudFileAllocationType));

	// Get table from priority object
	CrudRequest pullRequest = create_crud_request( 0, CRUD_READ, sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES,
			CRUD_PRIORITY_OBJECT, 0 );
	CrudResponse pullResponse = crud_bus_request( pullRequest, tempBuff );

	struct GenResponse response;

	// Check to make sure the CRUD_READ succeeded and extract the values
	extract_crud_response( pullResponse, &response.objectId, &response.request, &response.length,
			&response.flag, &response.succeed );
	if( response.succeed != 0 ) {
		return -1;
	}
	
	// Copy info to table
	memcpy(crud_file_table, tempBuff, CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType));
	
	// Free memory
	free(tempBuff);
	tempBuff = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {
	// Create temp buffer
	void* tempBuff = calloc(CRUD_MAX_TOTAL_FILES, sizeof(CrudFileAllocationType));
	memcpy(tempBuff, crud_file_table, CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType));

	// Create CRUD_UPDATE request to write to file
	CrudRequest updateRequest = create_crud_request( 0, CRUD_UPDATE, sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES,
			CRUD_PRIORITY_OBJECT, 0 );
	CrudResponse updateResponse = crud_bus_request( updateRequest, tempBuff );

	struct GenResponse response;

	// Check to make sure the CRUD_UPDATE succeeded and extract the values
	extract_crud_response( updateResponse, &response.objectId, &response.request, &response.length,
			&response.flag, &response.succeed );
	if( response.succeed != 0 ) {
		return -1;	
	}

	// Create CRUD_CLOSE request to save to file
	CrudRequest closeRequest = create_crud_request( 0, CRUD_CLOSE, 0, CRUD_NULL_FLAG, 0 );
	CrudResponse closeResponse = crud_bus_request( closeRequest, NULL );

	// Check to make sure the CRUD_CLOSE succeeded and extract the values
	extract_crud_response( closeResponse, &response.objectId, &response.request, &response.length,
			&response.flag, &response.succeed );
	if( response.succeed != 0 ) {
		return -1;	
	}

	// Free memory
	free(tempBuff);
	tempBuff = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file and returns a file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

int16_t crud_open(char *path) {
	int i;

	// Iterate through the table to see if file exists
	for( i=0; i<CRUD_MAX_TOTAL_FILES; i++ ){
		if(strcmp(path,crud_file_table[i].filename)==0){
			crud_file_table[i].open = 1;
			crud_file_table[i].position = 0;
			return i;
		} else if (strcmp("",crud_file_table[i].filename)==0){
			break;
		}
	}

	// Create CRUD_CREATE request and call it to make a object of 0 size
	CrudRequest createRequest = create_crud_request( 0, CRUD_CREATE, 0, 0, 0 );
	CrudResponse createResponse = crud_bus_request( createRequest, NULL );
	
	struct GenResponse response;

	// Check to make sure that the CRUD_CREATE succeeded
	extract_crud_response( createResponse, &response.objectId, &response.request, &response.length, 
			&response.flag, &response.succeed );
	if(response.succeed == 0) { // If succeeded, assign values to objectId and length
		int fd = i;
		strcpy(crud_file_table[fd].filename,path);
		crud_file_table[fd].open = 1;
		crud_file_table[fd].object_id = response.objectId;
		crud_file_table[fd].length = 0;
		crud_file_table[fd].position = 0;
		return fd;
	} else { 
		return -1;
	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fh) {
	if (fh<0 || fh>=CRUD_MAX_TOTAL_FILES){
		return -1;
	} else {
		crud_file_table[fh].open = 0;
		return 0;
	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
	// Allocate memory to work with
	char *tempBuffer = (char*) malloc( crud_file_table[fd].length*sizeof(char) );

	// Create CRUD_READ request and call it
	CrudRequest readRequest = create_crud_request( crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length, 0, 0 );
	CrudResponse readResponse = crud_bus_request( readRequest, tempBuffer );

	struct GenResponse response;

	// Check to make sure the CRUD_READ succeeded and extract the values
	extract_crud_response( readResponse, &response.objectId, &response.request, &response.length,
			&response.flag, &response.succeed );
	if( response.succeed == 0 ){
		if( (count+crud_file_table[fd].position)>=crud_file_table[fd].length ){ // In the case that there are not enough bytes
			memcpy( buf, &tempBuffer[crud_file_table[fd].position], (crud_file_table[fd].length-crud_file_table[fd].position));
			int tempCount = crud_file_table[fd].length - crud_file_table[fd].position;
			crud_file_table[fd].position = crud_file_table[fd].length;
			free(tempBuffer);
			tempBuffer = NULL;
			return tempCount;
		} else {
			memcpy( buf, &tempBuffer[crud_file_table[fd].position], count );
			crud_file_table[fd].position += count;
			free(tempBuffer);
			tempBuffer = NULL;
			return count;
		}
	} else {
		free(tempBuffer);
		tempBuffer = NULL;
		return -1;
	}
}

//////////////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count) {
	// Allocate memory to work with
	char *readBuffer = (char *)malloc(crud_file_table[fd].length*sizeof(char));
	char *tempBuffer;
	if (crud_file_table[fd].length >= (count+crud_file_table[fd].position)){
		tempBuffer = (char *)malloc(crud_file_table[fd].length*sizeof(char));
	} else {
		tempBuffer = (char *)malloc((count+crud_file_table[fd].position)*sizeof(char));
	}

	// Must first read file to prepare for write
	CrudRequest readRequest = create_crud_request( crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length, 0, 0 );
	CrudResponse readResponse = crud_bus_request( readRequest, readBuffer );

	struct GenResponse readResponseExtract;

	// Check to make sure the file was read properly
	extract_crud_response( readResponse, &readResponseExtract.objectId, &readResponseExtract.request,
			&readResponseExtract.length, &readResponseExtract.flag, &readResponseExtract.succeed );
	if( readResponseExtract.succeed == 0 ) {
		// Check the length of the final file
		if( crud_file_table[fd].length >= (count + crud_file_table[fd].position) ) { // If it is the same size
			// Prepare buffer to be sent
			memcpy( tempBuffer, readBuffer, crud_file_table[fd].length);
			memcpy( &tempBuffer[crud_file_table[fd].position], buf, count );

			// Create CRUD_UPDATE request to write to file
			CrudRequest updateRequest = create_crud_request( crud_file_table[fd].object_id, CRUD_UPDATE,
					crud_file_table[fd].length, 0, 0 );
			CrudResponse updateResponse = crud_bus_request( updateRequest, tempBuffer );

			struct GenResponse updateResponseExtract;

			// Make sure write succeeded
			extract_crud_response( updateResponse, &updateResponseExtract.objectId, &updateResponseExtract.request,
					&updateResponseExtract.length, &updateResponseExtract.flag, &updateResponseExtract.succeed );
			if( updateResponseExtract.succeed == 0 ) {
				crud_file_table[fd].position += count;

				free(tempBuffer);
				free(readBuffer);
				tempBuffer = NULL;
				readBuffer = NULL;
				return count;
			} else {
				free(readBuffer);
				free(tempBuffer);
				tempBuffer = NULL;
				readBuffer = NULL;
				return -1;
			}
		} else { // In the case size increases, we must delete the current file and make new one
			// Create CRUD_DELETE request
			memcpy(tempBuffer, readBuffer, (crud_file_table[fd].position + count));
			CrudRequest deleteRequest = create_crud_request( crud_file_table[fd].object_id, CRUD_DELETE, 
					crud_file_table[fd].length, 0, 0 );
			CrudResponse deleteResponse = crud_bus_request( deleteRequest, NULL );

			struct GenResponse deleteResponseExtract;

			// Make sure delete succeeded
			extract_crud_response( deleteResponse, &deleteResponseExtract.objectId, &deleteResponseExtract.request,
					&deleteResponseExtract.length, &deleteResponseExtract.flag, &deleteResponseExtract.succeed );
			if( deleteResponseExtract.succeed == 0 ){
				// Prepare buffer for writing
				memcpy( &tempBuffer[crud_file_table[fd].position], buf, count );

				// Create CRUD_CREATE request
				CrudRequest createRequest = create_crud_request( 0, CRUD_CREATE,
						(count + crud_file_table[fd].position), 0, 0 );
				CrudResponse createResponse = crud_bus_request( createRequest, tempBuffer );

				struct GenResponse createResponseExtract;

				// Make sure the create succeeded
				extract_crud_response( createResponse, &createResponseExtract.objectId, &createResponseExtract.request,
						&createResponseExtract.length, &createResponseExtract.flag, &createResponseExtract.succeed );
				if( createResponseExtract.succeed == 0 ) {
					crud_file_table[fd].object_id = createResponseExtract.objectId;
					crud_file_table[fd].length = createResponseExtract.length;
					crud_file_table[fd].position += count;

					free(tempBuffer);
					free(readBuffer);
					tempBuffer = NULL;
					readBuffer = NULL;
					return count;
				} else {
					free(readBuffer);
					free(tempBuffer);
					readBuffer = NULL;
					tempBuffer = NULL;
					return -1;
				}
			} else {
				free(readBuffer);
				free(tempBuffer);
				readBuffer = NULL;
				tempBuffer = NULL;
				return -1;
			}
		}
	} else {
		free(readBuffer);
		free(tempBuffer);
		readBuffer = NULL;
		tempBuffer = NULL;
		return -1;
	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {
	// Check if the loc is a valid location
	if(loc <= crud_file_table[fd].length) { // If yes, move position to loc
		crud_file_table[fd].position = loc;
		return 0;
	} else { // Else, fail
		return -1;
	}

}

////////////////////////////////////////////////////////////////////////////////
//
// Function	: create_crud_request
// Description	: Create a CrudRequest based on inputs to be used in crud_bus_request
//
// Inputs	: OID - objectID of the file in object store (if applicable)
// 		  req - request type, ie CRUD_INIT
// 		  length - length of the input (if applicable)
// 		  flag - not used
// 		  result - not used
// Outputs	: int64_t in the form of a CrudRequest to be used in crud_bus_request

CrudRequest create_crud_request( int32_t OID, int req, int32_t length, int flag, int result ) {
	int64_t request = 0; // Instantialize int64_t to hold values

	request = request+OID; // place in the OID

	request = request<<4; 
	request = request + (req&((1<<4)-1)); // place in request type

	request = request<<24;
	request = request + (length&((1<<24)-1)); // place in length

	request = request << 3;
	request = request + (flag&((1<<5)-1)); // place in flag

	request = request << 1; //skip result

	return (CrudRequest) request;
}

////////////////////////////////////////////////////////////////////////////////////
//
// Function	: extract_crud_response
// Description	: Takes a CrudResponse and extracts the information into the different components
//
// Inputs	: response - the CrudResponse to be analyzed
// 		  OID - objectID of the file in object store
// 		  req - the request that was called
//		  length - length of the file 
//		  flag - not used
//		  result - 0 if successful execution, -1 if failed 
// Outputs	: notihng

void extract_crud_response( CrudResponse response, int32_t *OID, int *req, int32_t *length, int *flag, int *result ) {
	*OID = (int64_t) response>>32;
	*req = (int64_t) (response & (((int64_t)1<<32)-1))>>28;
	*length = (int64_t) (response & ((1<<28)-1))>>4;
	*flag = (int64_t) (response & ((1<<4)-1))>>1;
	*result = (int64_t) response & 1;
}

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_bus_request(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}

































