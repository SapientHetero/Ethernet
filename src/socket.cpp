/* Copyright 2018 Paul Stoffregen
*
* Permission is hereby granted, free of charge, to any person obtaining a copy of this
* software and associated documentation files (the "Software"), to deal in the Software
* without restriction, including without limitation the rights to use, copy, modify,
* merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to the following
* conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
* INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
* PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
* OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include <Arduino.h>
#include "Ethernet.h"
#include "utility/w5100.h"

/* The following won't compile and I don't yet know why
//#define LOGERROR
#ifdef LOGERROR
#define ERROR_PRINT(x)          logError(x, false, __FILE__, __LINE__, __func__)

#define ERROR_PRINTDEC(x) {\
  char EPDnumber[41] = "";\
  itoa(x, EPDnumber, 10);\
  logError(EPDnumber, false, __FILE__, __LINE__, __func__)\
}

#define ERROR_PRINTHEX(x) {\
  char EPDnumber[9] = "";\
  ultoa((unsigned long)x, EPDnumber, 16);\
  logError(EPDnumber, false, __FILE__, __LINE__, __func__);\
}

#define ERROR_PRINTLN(x)        logError(x, true, __FILE__, __LINE__, __func__)

extern void logError(const char* errMsg, bool addNewLine, const char *fileNm, int lineNo, const char *fnctn);
extern void logError(uint8_t EPDnum, bool addNewLine, const char *fileNm, int lineNo, const char *fnctn);
#endif

*/

/*extern "C" {
#include "wdtFunctions.h"					////// WARNING! A copy of wdtFunctions had to be put in Ethernet/src directory to make this work. Make sure to update it if the main version ever changes
}*/

#if ARDUINO >= 156 && !defined(ARDUINO_ARCH_PIC32)
extern void yield(void);
#else
#define yield()
#endif

//#define DEBUG
#ifdef DEBUG
#define DEBUG_PRINT(x)       Serial.print (x)
#define DEBUG_PRINTLN(x)     Serial.println (x)
#define DEBUG_PRINTDEC(x)    Serial.print (x, DEC)
#define DEBUG_PRINTLNDEC(x)  Serial.println (x, DEC)
#define DEBUG_PRINTHEX(x)    Serial.print (x, HEX)
#define DEBUG_PRINTLNHEX(x)  Serial.println (x, HEX)
#define DEBUG_FLUSH()        Serial.flush()
#else
#define DEBUG_PRINT(x)
#define DEBUG_PRINTLN(x) 
#define DEBUG_PRINTDEC(x)
#define DEBUG_PRINTLNDEC(x)
#define DEBUG_PRINTHEX(x)
#define DEBUG_PRINTLNHEX(x)
#define DEBUG_FLUSH()
#endif

// TODO: randomize this when not using DHCP, but how?
static uint16_t local_port = 49152;  // 49152 to 65535

typedef struct {
	uint16_t RX_RSR; // Number of bytes received
	uint16_t RX_RD;  // Address to read
	uint16_t TX_FSR; // Free space ready for transmit
	uint16_t  RX_inc; // how much have we advanced RX_RD		rs: changed to uint16_t 2-15-2020
} socketstate_t;

static socketstate_t state[MAX_SOCK_NUM];


static uint16_t getSnTX_FSR(uint8_t s);
static uint16_t getSnRX_RSR(uint8_t s);
static void write_data(uint8_t s, uint16_t offset, const uint8_t* data, uint16_t len);
static void read_data(uint8_t s, uint16_t src, uint8_t* dst, uint16_t len);



/*****************************************/
/*          Socket management            */
/*****************************************/


void EthernetClass::socketPortRand(uint16_t n)
{
	n &= 0x3FFF;
	local_port ^= n;
	//Serial.printf("socketPortRand %d, srcport=%d\n", n, local_port);
}

uint8_t EthernetClass::socketBegin(uint8_t protocol, uint16_t port)
{
	uint8_t s, status[MAX_SOCK_NUM], chip, maxindex = MAX_SOCK_NUM;

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return MAX_SOCK_NUM; // immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4; // W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	// look at all the hardware sockets, use any that are closed (unused)
	for (s = 0; s < maxindex; s++) {
		status[s] = W5100.readSnSR(s);
		if (status[s] == SnSR::CLOSED) goto makesocket;
	}
	SPI.endTransaction();
	//DEBUG_PRINTLN("****************************** socketBegin - ALL SOCKETS ARE IN USE ******************************");
	return MAX_SOCK_NUM; // all sockets are in use
makesocket:
	//Serial.printf("W5000socket %d\n", s);
	EthernetServer::server_port[s] = 0;
	//delayMicroseconds(250); // TODO: is this needed??  -rs 11Feb2019
	W5100.writeSnMR(s, protocol);
	W5100.writeSnIR(s, 0xFF);
	if (port > 0) {
		W5100.writeSnPORT(s, port);
	}
	else {
		// if don't set the source port, set local_port number.
		if (++local_port < 49152) local_port = 49152;
		W5100.writeSnPORT(s, local_port);
	}
	W5100.execCmdSn(s, Sock_OPEN);
	uint8_t expected = SnSR::INIT;		// if TCP, expected SnSR value is SnSR::INIT - added 1/10/2019
	if (protocol == SnMR::UDP)			// but if UDP, expected SnSR value is SnSR::UDP - added 1/10/2019
		expected = SnSR::UDP;
	if (!W5100.waitForCmd(s, expected)) {
		DEBUG_PRINTLN("socketBegin() - timeout waiting for socket to open");
	}
	state[s].RX_RSR = 0;
	state[s].RX_RD = W5100.readSnRX_RD(s); // always zero?
	state[s].RX_inc = 0;
	state[s].TX_FSR = 0;
	//Serial.printf("W5000socket prot=%d, RX_RD=%d\n", W5100.readSnMR(s), state[s].RX_RD);
	SPI.endTransaction();
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket was opened to support socket management.
	return s;
}

// multicast version to set fields before open  thd
uint8_t EthernetClass::socketBeginMulticast(uint8_t protocol, IPAddress ip, uint16_t port)
{
	uint8_t s, status[MAX_SOCK_NUM], chip, maxindex = MAX_SOCK_NUM;

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return MAX_SOCK_NUM; // immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4; // W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	// look at all the hardware sockets, use any that are closed (unused)
	for (s = 0; s < maxindex; s++) {
		status[s] = W5100.readSnSR(s);
		if (status[s] == SnSR::CLOSED) goto makesocket;
	}
	//Serial.printf("W5000socket step2\n");
	SPI.endTransaction();
	return MAX_SOCK_NUM; // all sockets are in use
makesocket:
	//Serial.printf("W5000socket %d\n", s);
	EthernetServer::server_port[s] = 0;
	delayMicroseconds(250); // TODO: is this needed??
	W5100.writeSnMR(s, protocol);
	W5100.writeSnIR(s, 0xFF);
	if (port > 0) {
		W5100.writeSnPORT(s, port);
	}
	else {
		// if don't set the source port, set local_port number.
		if (++local_port < 49152) local_port = 49152;
		W5100.writeSnPORT(s, local_port);
	}
	// Calculate MAC address from Multicast IP Address
	byte mac[] = { 0x01, 0x00, 0x5E, 0x00, 0x00, 0x00 };
	mac[3] = ip[1] & 0x7F;
	mac[4] = ip[2];
	mac[5] = ip[3];
	W5100.writeSnDIPR(s, ip.raw_address());   //239.255.0.1
	W5100.writeSnDPORT(s, port);
	W5100.writeSnDHAR(s, mac);
	W5100.execCmdSn(s, Sock_OPEN);
	state[s].RX_RSR = 0;
	state[s].RX_RD = W5100.readSnRX_RD(s); // always zero?
	state[s].RX_inc = 0;
	state[s].TX_FSR = 0;
	//Serial.printf("W5000socket prot=%d, RX_RD=%d\n", W5100.readSnMR(s), state[s].RX_RD);
	SPI.endTransaction();
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket was opened to support socket management.
	return s;
}
// Return the socket's status
//
uint8_t EthernetClass::socketStatus(uint8_t s)
{
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	uint8_t status = W5100.readSnSR(s);
	SPI.endTransaction();
	return status;
}

// Immediately close.  If a TCP connection is established, the
// remote host is left unaware we closed.
//
void EthernetClass::socketClose(uint8_t s)
{
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.execCmdSn(s, Sock_CLOSE);
	SPI.endTransaction();
	DEBUG_PRINT("socketClose() - closing socket #");
	DEBUG_PRINTLN(s);
	if (!W5100.waitForCmd(s, SnSR::CLOSE_WAIT, SnSR::CLOSED, SnSR::FIN_WAIT)) {
		DEBUG_PRINTLN("socketClose() - timeout waiting for socket to close");
	}
	else {
		DEBUG_PRINT("socketClose() - Socket #");
		DEBUG_PRINT(s);
		DEBUG_PRINTLN(" closed");
	}
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket was closed to support socket management.
}

// Immediately close.  If a TCP connection is established, the
// remote host is left unaware we closed.
//
void EthernetClass::socketCloseAsync(uint8_t s)
{
	DEBUG_PRINT("socketCloseAsync() - closing socket #");
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.execCmdSn(s, Sock_CLOSE);
	SPI.endTransaction();
	DEBUG_PRINT("socketCloseAsync() - closing socket #");
	DEBUG_PRINTLN(s);
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket was closed to support socket management.
}

// Place the socket in listening (server) mode
//
uint8_t EthernetClass::socketListen(uint8_t s)
{
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	if (W5100.readSnSR(s) != SnSR::INIT) {
		SPI.endTransaction();
		return 0;
	}
	W5100.execCmdSn(s, Sock_LISTEN);
	SPI.endTransaction();
	if (!W5100.waitForCmd(s, SnSR::LISTEN)) {
		DEBUG_PRINTLN("socketListen() - timeout waiting for socket to enter listen mode");
	}
	DEBUG_PRINT("socketListen() - now listening on socket #");
	DEBUG_PRINTLN(s);
	return 1;
}


// establish a TCP connection in Active (client) mode.
//
void EthernetClass::socketConnect(uint8_t s, uint8_t* addr, uint16_t port)
{
	// set destination IP
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.writeSnDIPR(s, addr);
	W5100.writeSnDPORT(s, port);
	delay(90);				// +rs 1/10/2019 
	W5100.execCmdSn(s, Sock_CONNECT);
	SPI.endTransaction();
	//if (!W5100.waitForCmd(s, SnSR::ESTABLISHED)) {
	//	DEBUG_PRINTLN("socketConnect() - timeout waiting for socket to connect");
	//}
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket connected to support socket management.
}



// Gracefully disconnect a TCP connection.
//
void EthernetClass::socketDisconnect(uint8_t s)
{
	DEBUG_PRINT("socketDisconnect() - disconnecting socket #");
	DEBUG_PRINT(s);
	DEBUG_PRINT(" with status = 0x");
	DEBUG_PRINTLNHEX(socketStatus(s));
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);		// start SPI transaction
	W5100.execCmdSn(s, Sock_DISCON);					// send socket disconnect command
	SPI.endTransaction();								// and end SPI transaction
	if (!W5100.waitForCmd(s, SnSR::TIME_WAIT, SnSR::CLOSED, SnSR::FIN_WAIT)) {			// wait for command to complete, as indicated by Socket n Status Register
		DEBUG_PRINTLN("socketDisconnect() - timeout waiting for socket to disconnect");
	}
	else {
		DEBUG_PRINT("socketDisconnect() - Socket #");
		DEBUG_PRINT(s);
		DEBUG_PRINTLN(" disconnected");
	}
	EthernetClass::lastSocketUse[s] = millis();			//     record time at which it was sent so we can send CLOSE command if peer doesn't respond within timeout period...
}

// Gracefully disconnect a TCP connection without waiting for completion.
//
void EthernetClass::socketDisconnectAsync(uint8_t s)
{
	DEBUG_PRINT("socketDisconnectAsync() - disconnecting socket #");
	DEBUG_PRINT(s);
	DEBUG_PRINT(" with status = 0x");
	DEBUG_PRINTLNHEX(socketStatus(s));
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);		// start SPI transaction
	W5100.execCmdSn(s, Sock_DISCON);					// send socket disconnect command
	SPI.endTransaction();								// and end SPI transaction
	EthernetClass::lastSocketUse[s] = millis();			//     record time at which it was sent so we can send CLOSE command if peer doesn't respond within timeout period...
}

/*****************************************/
/*    Socket Data Receive Functions      */
/*****************************************/


static uint16_t getSnRX_RSR(uint8_t s)
{
#if 1
	uint16_t val, prev;

	prev = W5100.readSnRX_RSR(s);
	while (1) {
		val = W5100.readSnRX_RSR(s);
		if (val == prev) {
			return val;
		}
		prev = val;
	}
#else
	uint16_t val = W5100.readSnRX_RSR(s);
	return val;
#endif
}

static void read_data(uint8_t s, uint16_t src, uint8_t* dst, uint16_t len)
{
	uint16_t size;
	uint16_t src_mask;
	uint16_t src_ptr;

	//Serial.printf("read_data, len=%d, at:%d\n", len, src);
	src_mask = (uint16_t)src & W5100.SMASK;
	src_ptr = W5100.RBASE(s) + src_mask;

	if (W5100.hasOffsetAddressMapping() || src_mask + len <= W5100.SSIZE) {
		W5100.read(src_ptr, dst, len);
	}
	else {
		size = W5100.SSIZE - src_mask;
		W5100.read(src_ptr, dst, size);
		dst += size;
		W5100.read(W5100.RBASE(s), dst, len - size);
	}
}

// Receive data.  Returns size, or -1 for no data, or 0 if connection closed
//
int EthernetClass::socketRecv(uint8_t s, uint8_t* buf, int16_t len)
{
	// Check how much data is available
	int ret = state[s].RX_RSR;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	if (ret < len) {
		uint16_t rsr = getSnRX_RSR(s);
		ret = rsr - state[s].RX_inc;
		state[s].RX_RSR = ret;
		//Serial.printf("Sock_RECV, RX_RSR=%d, RX_inc=%d\n", ret, state[s].RX_inc);
	}
	if (ret == 0) {
		// No data available.
		uint8_t status = W5100.readSnSR(s);
		if (status == SnSR::LISTEN || status == SnSR::CLOSED ||
			status == SnSR::CLOSE_WAIT) {
			/////////////////////// Should probably add FIN_WAIT, CLOSING and LAST_ACK to this list of statuses indicating socket is closing
			// The remote end has closed its side of the connection,
			// so this is the eof state.  Ditto for TIME_Wait +rs
			ret = 0;
		}
		else {
			// The connection is still up, but there's no data waiting to be read
			ret = -1;
		}
	}
	else {
		if (ret > len) ret = len; // more data available than buffer length
		uint16_t ptr = state[s].RX_RD;
		if (buf) read_data(s, ptr, buf, ret);
		ptr += ret;
		state[s].RX_RD = ptr;
		state[s].RX_RSR -= ret;
		uint16_t inc = state[s].RX_inc + ret;
		if (inc >= 250 || state[s].RX_RSR == 0) {
			state[s].RX_inc = 0;
			W5100.writeSnRX_RD(s, ptr);
			W5100.execCmdSn(s, Sock_RECV);
			//Serial.printf("Sock_RECV cmd, RX_RD=%d, RX_RSR=%d\n",
			//  state[s].RX_RD, state[s].RX_RSR);
		}
		else {
			state[s].RX_inc = inc;
		}
	}
	SPI.endTransaction();
	//Serial.printf("socketRecv, ret=%d\n", ret);
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket read data to support socket management.
	return ret;
}

uint16_t EthernetClass::socketRecvAvailable(uint8_t s)
{
	uint16_t ret = state[s].RX_RSR;
	if (ret == 0) {
		SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
		uint16_t rsr = getSnRX_RSR(s);
		SPI.endTransaction();
		ret = rsr - state[s].RX_inc;
		state[s].RX_RSR = ret;
		//Serial.printf("sockRecvAvailable s=%d, RX_RSR=%d\n", s, ret);
	}
	return ret;
}

// get the first byte in the receive queue (no checking)
//
uint8_t EthernetClass::socketPeek(uint8_t s)
{
	uint8_t b;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	uint16_t ptr = state[s].RX_RD;
	W5100.read((ptr & W5100.SMASK) + W5100.RBASE(s), &b, 1);
	SPI.endTransaction();
	return b;
}

/*****************************************/
/*    Socket Data Transmit Functions     */
/*****************************************/

static uint16_t getSnTX_FSR(uint8_t s)
{
	uint16_t val, prev;

	prev = W5100.readSnTX_FSR(s);
	while (1) {
		val = W5100.readSnTX_FSR(s);
		if (val == prev) {
			state[s].TX_FSR = val;
			return val;
		}
		prev = val;
	}
}


static void write_data(uint8_t s, uint16_t data_offset, const uint8_t* data, uint16_t len)
{
	uint16_t ptr = W5100.readSnTX_WR(s);
	ptr += data_offset;
	uint16_t offset = ptr & W5100.SMASK;
	uint16_t dstAddr = offset + W5100.SBASE(s);

	if (W5100.hasOffsetAddressMapping() || offset + len <= W5100.SSIZE) {
		W5100.write(dstAddr, data, len);
	}
	else {
		// Wrap around circular buffer
		uint16_t size = W5100.SSIZE - offset;
		W5100.write(dstAddr, data, size);
		W5100.write(W5100.SBASE(s), data + size, len - size);
	}
	ptr += len;
	W5100.writeSnTX_WR(s, ptr);
}


/**
* @brief	This function used to send the data in TCP mode
* @return	1 for success else 0.
*/
uint16_t EthernetClass::socketSend(uint8_t s, const uint8_t* buf, uint16_t len)
{
	uint8_t status = 0;
	uint16_t ret = 0;
	uint16_t freesize = 0;										// empty bytes in W5500 circular buffer 
	uint8_t ir;
	uint16_t remblock = 0;										// remaining bytes to be sent after partial buffer write
	uint32_t start;

	if (len > W5100.SSIZE) {
		ret = W5100.SSIZE; // check size not to exceed MAX size.
	}
	else {
		ret = len;
	}

	// read circular buffer free space
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	freesize = getSnTX_FSR(s);									// currently available space in socket n's circular write buffer
	status = W5100.readSnSR(s);
	SPI.endTransaction();
	remblock = ret;												// number of bytes that won't currently fit in circular buffer

	// if any free space is available, start.
	if (freesize < remblock) {									// if all "len" bytes can't be written...
		if (freesize > 0) {										//  but some space is available...
			SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
			write_data(s, 0, (uint8_t*)buf, freesize);			// make use of wait time by writing all that will fit, then write the rest when there's room
			W5100.writeSnIR(s, (SnIR::SEND_OK | SnIR::TIMEOUT));// clear SnIR SEND_OK and TIMEOUT bits before starting next send
			W5100.execCmdSn(s, Sock_SEND);
			SPI.endTransaction();
			remblock = ret - freesize;							// number of bytes that won't currently fit in circular buffer
		}
	// now wait for more space in circular buffer
		start = millis();										// record time loop is entered +rs 17Feb2019
		do {
			SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
			freesize = getSnTX_FSR(s);
			status = W5100.readSnSR(s);
			SPI.endTransaction();
			if ((status != SnSR::ESTABLISHED) && (status != SnSR::CLOSE_WAIT) || millis() - start > 1000)		// todo - implement user-configurable _timeout
				return ret - remblock;
			wdtClear();											// clear watchdog timer if needed
			yield();
		} while (freesize < remblock);							// loop until there's enough space or timeout occurs (in case peer process crashes without closing socket) +rs
	}

	// when space is available, write remainder of data
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	write_data(s, 0, (uint8_t*)(buf+len-remblock), remblock);
	W5100.writeSnIR(s, (SnIR::SEND_OK | SnIR::TIMEOUT));		// clear SnIR SEND_OK and TIMEOUT bits before starting next send+rs 18May2025
	W5100.execCmdSn(s, Sock_SEND);
	SPI.endTransaction();

	EthernetClass::lastSocketUse[s] = millis();					// record time at which socket sent data to support socket management.
	return ret;
}

int EthernetClass::socketSendAvailable(uint8_t s)
{
	uint8_t status = 0;
	uint16_t freesize = 0;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	freesize = getSnTX_FSR(s);
	status = W5100.readSnSR(s);
	SPI.endTransaction();
	if ((status == SnSR::ESTABLISHED) || (status == SnSR::CLOSE_WAIT)) {
		return freesize;
	}
	return -1;   // differentiates between 'no space available' and 'socket error state'
}

uint16_t EthernetClass::socketBufferData(uint8_t s, uint16_t offset, const uint8_t* buf, uint16_t len)
{
	//Serial.printf("  bufferData, offset=%d, len=%d\n", offset, len);
	uint16_t ret = 0;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	uint16_t txfree = getSnTX_FSR(s);
	if (len > txfree) {
		ret = txfree; // check size not to exceed MAX size.
	}
	else {
		ret = len;
	}
	write_data(s, offset, buf, ret);
	SPI.endTransaction();
	return ret;
}

bool EthernetClass::socketStartUDP(uint8_t s, uint8_t* addr, uint16_t port)
{
	if (((addr[0] == 0x00) && (addr[1] == 0x00) && (addr[2] == 0x00) && (addr[3] == 0x00)) ||
		((port == 0x00))) {
		return false;
	}
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.writeSnDIPR(s, addr);
	W5100.writeSnDPORT(s, port);
	SPI.endTransaction();
	return true;
}

bool EthernetClass::socketSendUDP(uint8_t s)
{
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.execCmdSn(s, Sock_SEND);

	/* +2008.01 bj */
	while ((W5100.readSnIR(s) & SnIR::SEND_OK) != SnIR::SEND_OK) {
		if (W5100.readSnIR(s) & SnIR::TIMEOUT) {
			/* +2008.01 [bj]: clear interrupt */
			W5100.writeSnIR(s, (SnIR::SEND_OK | SnIR::TIMEOUT));
			SPI.endTransaction();
			//Serial.printf("sendUDP timeout\n");
			return false;
		}
		SPI.endTransaction();
		wdtClear();      // clear watchdog timer if needed
		yield();
		SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	}

	/* +2008.01 bj */
	W5100.writeSnIR(s, SnIR::SEND_OK);
	SPI.endTransaction();

	//Serial.printf("sendUDP ok\n");
	/* Sent ok */
	return true;
}

void EthernetClass::manageSockets(EthernetServer* server, uint8_t maxListeners, uint8_t doNotDisconnectSocket) {

	// Sockets in CLOSE_WAIT state have received FIN from peer and are waiting for us to respond.
	// This function checks each socket and if it finds one in CLOSE_WAIT state with no data waiting to 
	// be read on it, sends the W5x00 a DISCON command that causes it to reply to the peer with a FIN.
	// When the peer responds with an ACK, the W5x00 will set the socket status to CLOSED, making it 
	// available for use. Applications can periodically call this function to improve responsiveness
	// compared to waiting until the library runs out of sockets before closing them. Replaces 'socket 
	// making' code in socketBegin() and socketBeginMulticast() with more responsive & standards-compliant
	// functionality.
	// added by Ron Sutton 2/2019.

	// added Denial of Service defense feature on October 26, 2019 in response to DDoS attack originating from Seoul, Korea and Amazon's AWS. Attack tied up sockets by sending SYN packets but never
	// responding with ACK to W5500's SYN/ACK response. This kept all sockets in SnSR::SYNRECV status and unable to connect to legitimate requests.
	/*	A SYNFLOOD attack occurs when an attacker repeatedly requests a socket connection by sending a SYN packet but fails to send an ACK to complete the connection, leaving the socket unavailable 
		until the ACK is timed out. This can quickly cripple a system with only 4 or 8 sockets.
		
		This version of socket.cpp detects two types of SYN FLOOD DOS attacks, localized and distributed, and takes defensive action to allow the system to continue operating during the attacks. 
		The former emanates from a one system and is often simulated using Kali's hping3 utility (in which case the attack appears to come from different IPs in the same subnet), while the latter 
		emanates from two or more different systems. 
		
		This feature can be enabled by defining ANTI_DENIAL_OF_SERVICE.
		
		When SOCKS_IN_SYN_THRESH sockets are consumed by incomplete socket connection requests (socket in SYNRECV state) for ATTACK_THRESHOLD_MS milliseconds, the system declares that a SYN FLOOD 
		attack is in progress and activates defensive measures. When an attack is declared, the system switches to a more aggressive socket connection timeout based on the average number of attacks
		in the current and previous monitoring intervals, and immediately rejects connection attempts from up to 20 attacking subnets without attempting to complete the connection. The result is 
		that the system closes sockets faster than attacker(s) can initiate new connection requests, leaving sockets available for normal operation.
		
		An attack is considered to have ended when the number of attacks per ATTACK_END_INTERVAL milliseconds drops below ATTACK_END_THRESHOLD. When this occurs, defenses remain active for a random 
		period of time between ATTACK_END_MIN_MS and ATTACK_END_MAX_MS so attackers can't deactivate defenses by pausing the attack for a known  period of time.

		Attacks can be simulated within the code by defining SIMULATE_RANDOM_DOS or SIMULATE_SUBNET_DOS for code testing purposes. Final testing was done via actual attacks using Kali's hping3 on the 
		same physical LAN as the system under test, which is a worst-case scenario due to low network latency. The system was able to successfully complete legitimate requests during the attack, 
		though with diminished responsiveness. The SYN FLOOD defense has successfully defended against external SYN FLOOD attacks as well.
	*/
#define ANTI_DENIAL_OF_SERVICE
#define NORMAL_SYN_TIMEOUT_MS 31800UL		// if connection is not established within 10 secs of first SYN packet, close socket
#define ATTACK_THRESHOLD_MS 1000UL			// if all sockets are in SYNRECV state for ATTACK_THRESHOLD_MS milliseconds, a SYN flood attack is in progress
#define ATTACK_SYN_TIMEOUT_MS 250UL			// if an attack is in progress, automatically close any socket that remains in SYNRECV state for at least ATTACK_ACK_TIMEOUT_MS milliseconds
#define SYN_ATTACK_INTERVAL_MS 1000UL		// record the number of SYN flood attacks from a given IP subnet over ATTACK_SUBNET_INTERVAL_MS milliseconds
#define	SUBNET_ATTACK_THRESHOLD 10			// number of SYN flood attack packets appearing to come from a given IP subnet per ATTACK_SUBNET_INTERVAL_MS milliseconds before it is considered to be participating in an attack
#define ATTACK_END_MIN_MS 15000UL			// minimum period of time with fewer than ATTACK_END_THRESHOLD SYN attacks/ATTACK_END_INTERVAL before attack is considered to have ended
#define ATTACK_END_MAX_MS 120000UL			// maximum period of time with fewer than ATTACK_END_THRESHOLD SYN attacks/ATTACK_END_INTERVAL before attack is considered to have ended
#define ATTACK_END_INTERVAL	1000UL			// interval over which total number of SYN attacks are monitored once an attack begins
#define ATTACK_END_THRESHOLD 0				// the number of SYN attacks per ATTACK_END_INTERVAL required to keep defensive measures active once triggered
#define MAX_ATTACK_SUBNETS 20				// maximum number of attacking subnets tracked
#define SOCKS_IN_SYN_THRESH 6				// number of sockets that can be simultaneously in SYN_RECV state before SYN attack is declared
#define SYN_ATTACK_THRESH 16				// number of attacks required in 2 monitoring intervals for a SYN attack to be declared

//#define SIMULATE_SUBNET_DOS					// when defined, randomly map 192.168.1.1 IP address to attacking subnets
//#define SIMULATE_RANDOM_DOS					// when defined, randomly map 192.168.1.1 IP address to random IP addresses

	static uint8_t attack_subnet_count = 0;	// number of subnets identified as being involved in SYN flood	attack
	uint32_t attacker_subnet;				// the subnet to which the current (potential) attacker belongs
	uint8_t chip;							// type of Wiznet chip in system
	static bool connecting[MAX_SOCK_NUM] = { false, false, false, false, false, false, false, false }; // indicates whether socket was in the process of connecting last time through manageSockets()
	bool connection_timeout;				// indicates whether current socket is stuck in SYN_RECV state
	static uint32_t current_interval_attacks; // the overall number of attacks in the current attack monitoring interval
	uint8_t disconnectableSockets = 0;		// number of sockets in a condition that allows them to be safely disconnected
	uint8_t established = 0;				// number of sockets in ESTABLISHED state (other than LANclient socket)
	uint16_t fewest_attacks;				// used to identify subnet in SYN_attack_subnets[] for reuse when array is full
	uint8_t i;								// general purpose loop counter
	static uint32_t last_SYN_attack_time;	// time at which last SYN attack packet was detected
	static uint32_t last_SYN_update = 0;	// time at which last SYN attack monitoring update was performed
	int8_t listening = 0;					// number of sockets in LISTEN state
	uint32_t maxAge;						// the 'age' of the socket in a 'disconnectable' state that was last used the longest time ago
	uint8_t maxindex = MAX_SOCK_NUM;		// the number of sockets in use on the Wiznet chip
	uint32_t minForceCloseAge;				// minimum age at which idle socket will be forced to close
	uint8_t oldest;							// the socket number of the 'oldest' disconnectable socket
	static uint32_t previous_interval_attacks; // the overall number of attacks in the previous attack monitoring interval
	uint8_t remoteIParray[4];				// client IP address associated with a socket
	uint8_t s;								// loop counter used to index through sockets
	uint32_t socketAge[MAX_SOCK_NUM];		// array storing the age of sockets in a 'disconnectable' state
	int8_t socketsAvailable = 0;			// number of sockets available for use (i.e., in a CLOSED state)
	int8_t socketsDisconnecting = 0;		// number of sockets in the process of disconnecting, either upon entry or due to commands sent by this function
	uint8_t status;							// value of some socket's Sn_SR
	int8_t subnet_index;					// index of subnet element in SYN_attack_subnets[] corresponding to current socket's client IP address
	static struct {							// records information about subnets from which SYN flood attack appears to originate
		uint32_t subnet;					// first 3 octets of subnet IP address
		uint32_t last_attack_time;			// system time when last attack from this subnet was recorded
		uint16_t previous_interval_attacks;	// number of attacks from this subnet in previous monitoring interval
		uint16_t current_interval_attacks;	// number of attacks from this subnet in current monitoring interval
	} SYN_attack_subnets[MAX_ATTACK_SUBNETS] = {
		{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0},{0,0,0,0}
	};
	static bool SYN_Flood_Attack = false;	// indicates whether a SYN flood attack is currently thought to be underway
	static uint32_t SYN_Flood_Attack_end_delay; // random period during which no SYN attack packets must be received in order for attack to be considered ended
	uint8_t SYNRECV_count = 0;				// number of sockets in SnSR::SYNRECV state
	static uint32_t SYNRECV_timeout = NORMAL_SYN_TIMEOUT_MS; // timeout used to "recover" sockets abandoned by client in SYNRECV state. This value is reduced during SYN flood attacks to defend against random IP address attacks.
	int8_t toDisconnect;					// number of sockets that will be sent DISCON commands

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return;															// immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4;												// W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);

//#define IORATETEST 1

#ifdef IORATETEST

#define NSTEPS 13
	static uint16_t microDelayArray[NSTEPS] = { 25000, 12500, 625, 312, 156, 78, 39, 20, 10, 5, 3, 2, 1 };
#define STEP_INTERVAL_MS 5000
	static uint8_t stepno = 0;
	static uint32_t lastStepTime = millis();

	if (millis() - lastStepTime > STEP_INTERVAL_MS) {
		W5100.setInterCmdDelay(microDelayArray[stepno]);
		lastStepTime = millis();
		Serial.print("Min time between W5500 commands set to ");
		Serial.println(microDelayArray[stepno]);
		DEBUG_PRINT("Min time between W5500 commands set to ");
		DEBUG_PRINTLN(microDelayArray[stepno]);
		stepno++;
		if (stepno == NSTEPS)
			stepno = 0;
	}

#endif

	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);								// begin SPI transaction
	// look at all the hardware sockets, record and take action based on current states
	for (s = 0; s < maxindex; s++) {											// for each hardware socket ...
		socketAge[s] = 0;														// initialize the socketAge array to a state that indicates no "ESTABLISHED" sockets are available to disconnect
		status = W5100.readSnSR(s);												//  get socket status...
		if (connecting[s] && status != SnSR::SYNRECV) {							//  if socket is not in the process of responding to a connection request...
			connecting[s] = false;												//   update its state accordingly.
			DEBUG_PRINT("Socket connect time (ms) = ");
			DEBUG_PRINTLN(millis() - EthernetClass::lastSocketUse[s]);
		}
		if (status == SnSR::CLOSE_WAIT && socketRecvAvailable(s) == 0) {		//   if CLOSE_WAIT and no data available to read...
			W5100.execCmdSn(s, Sock_DISCON);								    //    send DISCON command...
			EthernetClass::lastSocketUse[s] = millis();							//     record time at which it was sent so we can send CLOSE command if peer doesn't respond...
			socketsDisconnecting++;												//      and increment the number of sockets that are disconnecting.
			DEBUG_PRINT("EthernetClass::manageSockets() - DISCON command sent for CLOSE_WAIT socket #");
			DEBUG_PRINTLN(s);
		}
		else if (status == SnSR::CLOSED) {									//   else if closed socket...
			socketsAvailable++;													//    increment available sockets...
		}
		else if (status == SnSR::FIN_WAIT || status == SnSR::CLOSING ||		//   else if socket is in the process of closing...
			status == SnSR::TIME_WAIT || status == SnSR::LAST_ACK) {
			socketsDisconnecting++;												//    increment the number of sockets that are disconnecting...
			if (millis() - EthernetClass::lastSocketUse[s] > 250) {				//     if it's been more than 250 milliseconds since disconnect command was sent...
				W5100.execCmdSn(s, Sock_CLOSE);									//	    send CLOSE command...
				EthernetClass::lastSocketUse[s] = millis();						//       and record time at which it was sent so we don't do it repeatedly.
				DEBUG_PRINT("EthernetClass::manageSockets() - CLOSE command sent for TIMEWAIT/CLOSING socket #");
				DEBUG_PRINTLN(s);
			}
		}
		else if (status == SnSR::ESTABLISHED && s != doNotDisconnectSocket && //   else if socket is connected and caller hasn't requested to keep it that way...
			socketRecvAvailable(s) == 0) {									//    and socket has no data waiting to be read...
			established++;														//	    count it...
			socketAge[s] = millis() - EthernetClass::lastSocketUse[s];			//       and record time since last socket use.
		}
		else if (status == SnSR::LISTEN) {									//   else if socket is in LISTEN state...
			listening++;														//    count it.
#ifdef ANTI_DENIAL_OF_SERVICE					// anti-Denial-of-Service code follows
		}
		else if (status == SnSR::SYNRECV) {									//   else if socket is in the process of connecting...
			W5100.readSnDIPR(s, remoteIParray);									//    get client IP address...
			//DEBUG_PRINT("remoteIParray = ");
			//DEBUG_PRINTLN(IPAddress(remoteIParray));
			attacker_subnet = 0 | remoteIParray[0] << 24 | remoteIParray[1] << 16 | remoteIParray[2] << 8;	//    extract attacker's subnet

#ifdef SIMULATE_SUBNET_DOS						// simulate denial-of-service attack from up to 20 different subnets
			if (attacker_subnet == 0xC0A80100) {								// if subnet = 192.168.1.0, attack is coming from within LAN...
				attacker_subnet = 52 << 24 | 192 << 16 | random(1, 21) << 8;			//  randomly choose one of 20 subnets
			}
#else
#ifdef SIMULATE_RANDOM_DOS						// simulate denial-of-service attack from random IP addresses
			if (attacker_subnet == 0xC0A80100) {								// if subnet = 192.168.1.0, attack is coming from within LAN...
				attacker_subnet = 0;
				for (i = 0; i < 3; i++) {
					attacker_subnet = (attacker_subnet | random(1, 255)) << 8;	//  generate a randomly selected IP subnet instead of using subnet from client's remote IP
				}
			}
#endif		// SIMULATE_RANDOM_DOS
#endif		// SIMULATE_SUBNET_DOS

			/*			DEBUG_PRINT("attacker_subnet = ");
						DEBUG_PRINT((attacker_subnet & 0xff000000)>>24);
						DEBUG_PRINT(".");
						DEBUG_PRINT((attacker_subnet & 0x00ff0000)>>16);
						DEBUG_PRINT(".");
						DEBUG_PRINT((attacker_subnet & 0x0000ff00)>>8);
						DEBUG_PRINTLN(".0");
			*/

			connection_timeout = connecting[s] & (millis() - EthernetClass::lastSocketUse[s] > SYNRECV_timeout); // determine if connection has timed out

			if (!connecting[s]) {																//   if socket was not previously observed to be in connection process...
				connecting[s] = true;															//    note that it is now...
				EthernetClass::lastSocketUse[s] = millis(); 									//    record the current time so connection timeout can be detected...
			}

			// Identify attack subnet current socket's IP address may belong to
			if (SYN_Flood_Attack || connection_timeout) {										//    if SYN flood attack has been detected or socket connection has timed out...
				subnet_index = -1;																//     negative subnet_index indicates current socket is not in a previously-identified attack subnet.
				for (i = 0; i < attack_subnet_count; i++) {										//     determine whether the client IP address connecting to the socket is a member of a previously-identified attack subnet..
					if (attacker_subnet == SYN_attack_subnets[i].subnet) {						//      if so...
						subnet_index = i;														//       record its array index...
						break;																	//       and stop searching.
					}
				}
				// Close sockets that have timed out while connecting (attacks from random IPs) or are involved in an attack from specific subnets
				if ((SYN_Flood_Attack && subnet_index >= 0) || connection_timeout) {			//    if SYN flood attack has been detected and socket's client IP is in identified attack subnet or socket establishment has timed out...
					W5100.execCmdSn(s, Sock_CLOSE);												//	   send CLOSE command...
					EthernetClass::lastSocketUse[s] = millis();									//     record time at which it was sent so we don't do it repeatedly...
					last_SYN_attack_time = millis();											//     update time of last attack...
					socketsDisconnecting++;														//     increment the number of sockets that are disconnecting...
					connecting[s] = false;														//     and record that socket's no longer in a "connecting" state.
					current_interval_attacks++;													//     increment the number of attacks detected in the current monitoring interval...
					DEBUG_PRINT("EthernetClass::manageSockets() - SYN Flood CLOSE command sent for socket #");
					DEBUG_PRINT(s);
					DEBUG_PRINT(" with IP address ");
					DEBUG_PRINT(IPAddress(remoteIParray));
					DEBUG_PRINT(" belonging to attacking subnet ");
					DEBUG_PRINT((attacker_subnet & 0xff000000) >> 24);
					DEBUG_PRINT(".");
					DEBUG_PRINT((attacker_subnet & 0x00ff0000) >> 16);
					DEBUG_PRINT(".");
					DEBUG_PRINT((attacker_subnet & 0x0000ff00) >> 8);
					DEBUG_PRINTLN(".0");

					if (SYN_Flood_Attack && subnet_index >= 0) {								//     if  SYN flood attack has been detected and socket's client IP is in identified attack subnet...
						SYN_attack_subnets[subnet_index].current_interval_attacks++;			//		increment number of attacks from this subnet in current monitoring interval...
						SYN_attack_subnets[subnet_index].last_attack_time = millis();			//		update time of last attack from this subnet...
					}
					else if (SYN_Flood_Attack && subnet_index < 0) {							//	   otherwise, if socket's client IP is not in an identfied attack subnet...
						if (attack_subnet_count < MAX_ATTACK_SUBNETS) {				       		//      and if at least one empty slot is available in the array that records subnets participating in the attack...
							subnet_index = attack_subnet_count++;								//       then add the socket's subnet in the next empty array slot.
						}
						else {																//      else find a subnet to overwrite, making sure highly active subnets are not overwritten by random IPs mixed in with them...
							fewest_attacks = SYN_attack_subnets[0].previous_interval_attacks + SYN_attack_subnets[0].current_interval_attacks;
							subnet_index = 0;													//       set fewest to first array element and compare the rest to it...
							for (i = 1; i < MAX_ATTACK_SUBNETS; i++) {							//       find subnet entry with fewest attacks over the last 2 monitoring intervals...
								uint16_t attacks = SYN_attack_subnets[i].previous_interval_attacks + SYN_attack_subnets[i].current_interval_attacks;
								if (attacks < fewest_attacks) {									//        if current entry has fewer attacks than current lowest...
									fewest_attacks = attacks;									//         make it the new current lowest.
									subnet_index = i;
								}
								else if (attacks == fewest_attacks) {							//        but if the number of attacks is tied, keep the one that most recently sent an attack.
									if (SYN_attack_subnets[i].last_attack_time > SYN_attack_subnets[subnet_index].last_attack_time) {
										fewest_attacks = attacks;
										subnet_index = i;
									}
								}
							}
						}
						SYN_attack_subnets[subnet_index].subnet = attacker_subnet;				//		record socket's subnet in attacking subnets array...
						SYN_attack_subnets[subnet_index].previous_interval_attacks = 0;			//		initialize its previous monitoring interval attack count to zero...
						SYN_attack_subnets[subnet_index].current_interval_attacks = 1;			//      its current monitoring interval attack count to one...
						SYN_attack_subnets[subnet_index].last_attack_time = millis();			//		and record time of last attack from this subnet...
					}
				}
			}
			else {																			// socket not closed above
				SYNRECV_count++;																//    increment number of sockets currently in SYNRECV state.
			}
#endif		// ANTI_DENIAL_OF_SERVICE
		}
		else if (status != SnSR::CLOSED && status != SnSR::INIT &&							// else if socket is not in any known state, print debug message.
			status != SnSR::LISTEN && status != SnSR::SYNSENT &&
			status != SnSR::SYNRECV && status != SnSR::ESTABLISHED &&
			status != SnSR::FIN_WAIT && status != SnSR::CLOSING &&
			status != SnSR::TIME_WAIT && status != SnSR::CLOSE_WAIT &&
			status != SnSR::LAST_ACK && status != SnSR::UDP &&
			status != SnSR::IPRAW && status != SnSR::MACRAW &&
			status != SnSR::PPPOE) {
			DEBUG_PRINT("Unexpected status 0x");
			DEBUG_PRINTHEX(status);
			DEBUG_PRINT(" observed on socket #");
			DEBUG_PRINTLN(s);
		}
	}

	// begin listening on  CLOSED sockets, leaving at least one available for rapid LANclient connection changes and NTP updates
	while ((socketsAvailable > 1 && listening < maxListeners) ||				//  while more than 1 socket is in CLOSED state and less than maxListeners are in LISTEN state...
		(socketsAvailable > 0 && listening == 0)) {							//   OR at least ONE socket is available and NO sockets are listening...
		if (server != NULL) {													//     if server parameter isn't NULL...
			if (server->beginAsync()) {											//     start listening on a CLOSED socket...
				socketsAvailable--;												//      if successful, decrement number of available sockets...
				listening++;													//       and increment number of listeners.
			}
			else {
				break;															//      else break to avoid infinite loop.
			}
		}
		else {
			break;
		}
	}

#ifdef ANTI_DENIAL_OF_SERVICE
	// Detect SYN flood attack
	if (!SYN_Flood_Attack && (SYNRECV_count >= SOCKS_IN_SYN_THRESH ||					// if not currently in attack mode and either SOCKS_IN_SYN_THRESH or more sockets are stuck in SYNRECV state...
		previous_interval_attacks + current_interval_attacks >= SYN_ATTACK_THRESH)) {	// or the total number of attacks detected in the last 2 monitoring intervals exceeds SYN_ATTACK_THRESH...
		DEBUG_PRINTLN("SYN Flood Attack detected");
		SYN_Flood_Attack = true;														//  set flag indicating attack is in progress, thus enabling counter-measures...
		//SYNRECV_timeout = ATTACK_SYN_TIMEOUT_MS;										//  reduce timeout for sockets to remain in SYNRECV state to protect against attacks using random IP addresses...
		SYNRECV_timeout = 2 * SYN_ATTACK_INTERVAL_MS / (previous_interval_attacks + current_interval_attacks);	//  compute connection timeout based on SYN attack packet detection rate...
		last_SYN_attack_time = millis();												//  record time at which attack was detected...
		last_SYN_update = millis();														//  set up next attack monitoring update...
		SYN_Flood_Attack_end_delay = random(ATTACK_END_MIN_MS, ATTACK_END_MAX_MS);		//  and set random time at which SYN flood attack defenses are disabled following the apparent end of the attack.
		DEBUG_PRINT("SYN attack end delay = ");
		DEBUG_PRINTLN(SYN_Flood_Attack_end_delay);
	}

	// Detect END of SYN flood attack
	if (SYN_Flood_Attack) {														// if SYN flood attack is in progress...
		if (millis() - last_SYN_attack_time >= SYN_Flood_Attack_end_delay) {	//  check whether attack has ended...
			SYN_Flood_Attack = false;											//   if so, clear flag...
			attack_subnet_count = 0;											//   clear subnets involved in attack...
			current_interval_attacks = 0;										//   initialize attack counts for current...
			previous_interval_attacks = 0;										//   and previous monitoring intervals...
			SYNRECV_timeout = NORMAL_SYN_TIMEOUT_MS;							//   and reset SYN_RECV timeout to normal non-attack value.
			DEBUG_PRINTLN("SYN flood attack ended");
		}
	}

	// SYN flood attack monitoring
	if (millis() - last_SYN_update >= SYN_ATTACK_INTERVAL_MS) {													// if current monitoring interval has ended...
		last_SYN_update = millis();																				//  record time at which new monitoring interval began...
		uint32_t recent_attacks = previous_interval_attacks + current_interval_attacks;
		if (recent_attacks > 0)																				    //  if any attacks were detected in the last 2 monitoring cycles...
			SYNRECV_timeout = 2 * SYN_ATTACK_INTERVAL_MS / (recent_attacks);									//   update connection timeout based on SYN attack packet detection rate...
		for (i = 0; i < attack_subnet_count; i++) {																//  for each subnet involved in the attack...
			SYN_attack_subnets[i].previous_interval_attacks = SYN_attack_subnets[i].current_interval_attacks;	// move subnet current interval attack statistics to previous interval...
			SYN_attack_subnets[i].current_interval_attacks = 0;													// set subnet current interval statistics to zero...
		}
		previous_interval_attacks = current_interval_attacks;													// move overall current interval attack statistics to previous interval...
		current_interval_attacks = 0;																			// set overall current interval statistics to zero...
	}
#endif

	// Now disconnect idle sockets, starting with the one that's been idle the longest
	minForceCloseAge = 300000;													// unless sockets are needed immediately, leave them connected for 5 minutes after last use.

	if (listening <= 1 && socketsAvailable == 0 && socketsDisconnecting == 0) {	// if fewer than 2 sockets are listening and no sockets are free or disconnecting...
		if (listening == 0)														// if NO socket is listening...
			minForceCloseAge = 250;												//  force 'excess' sockets that have been idle at least 0.25 seconds to disconnect...
		else if (listening == 1)												// if only 1 socket is listening...
			minForceCloseAge = 1000;											//  force 'excess' sockets that have been idle at least 1 second to disconnect...

		toDisconnect = 2 - listening;												// set "toDisconnect" with goal of ensuring at least 2 sockets are listening at any given time...
		if (toDisconnect > established)											//  by setting it to the lesser of 'established'...
			toDisconnect = established;											//   and 2 minus the number of sockets currently listening.

		while (toDisconnect > 0) {												// while additional sockets are needed and eligible sockets remain...

			oldest = MAX_SOCK_NUM;												//  find the 'oldest' disconnectable socket...
			maxAge = 0;
			for (s = 0; s < maxindex; s++) {									//   by scanning the socketAge array populated above...
				if (socketAge[s] > maxAge) {									//    if current socket's "age" is greater than the max age recorded so far...
					oldest = s;													//     record the socket number...
					maxAge = socketAge[s];										//      and make its age the new max age.
				}
			}

			if (oldest != MAX_SOCK_NUM && maxAge > minForceCloseAge) {			// if an "oldest" socket is found that's been idle at least minForceCloseAge milliseconds...
				W5100.execCmdSn(oldest, Sock_DISCON);							//  send DISCON command...
				EthernetClass::lastSocketUse[oldest] = millis();				//   record time at which it was sent...
				socketAge[oldest] = 0;											//    mark the socket as no longer available to be disconnected...
				toDisconnect--;													//     and decrement the number of sockets remaining to disconnect.
				if (minForceCloseAge == 250)									//  if there were no sockets listening...
					minForceCloseAge = 1000;									//   increase minForceCloseAge to 1 second after closing one...
				else if (minForceCloseAge == 1000)								//  if there was one socket listening...
					minForceCloseAge = 300000;									//   increase minForceCloseAge to 300 seconds after closing one
				DEBUG_PRINT("EthernetClass::manageSockets() - DISCON command sent for socket #");
				DEBUG_PRINTLN(oldest);
			}
			else {															// otherwise...
				break;															//  break to avoid infinite loop.
			}
		}
	}

	SPI.endTransaction();														// when done with all sockets...
	return;																		//  return without waiting for socket(s) to close.
}


uint32_t EthernetClass::getWaitTime(void) {

	uint32_t t = W5100.getAvgWait();
	return t;

}

void EthernetClass::closeSockets(uint16_t port) {

	uint16_t p;
	uint8_t s;

	for (s = 0; s < MAX_SOCK_NUM; s++) {
		SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
		p = W5100.readSnPORT(s);
		SPI.endTransaction();
		if (port == p)
			EthernetClass::socketCloseAsync(s);
	}
}
