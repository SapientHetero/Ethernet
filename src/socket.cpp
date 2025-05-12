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
	uint8_t  RX_inc; // how much have we advanced RX_RD
} socketstate_t;

static socketstate_t state[MAX_SOCK_NUM];


static uint16_t getSnTX_FSR(uint8_t s);
static uint16_t getSnRX_RSR(uint8_t s);
static void write_data(uint8_t s, uint16_t offset, const uint8_t *data, uint16_t len);
static void read_data(uint8_t s, uint16_t src, uint8_t *dst, uint16_t len);



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
	uint8_t s, status[MAX_SOCK_NUM], chip, maxindex=MAX_SOCK_NUM;

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return MAX_SOCK_NUM; // immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4; // W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	// look at all the hardware sockets, use any that are closed (unused)
	for (s=0; s < maxindex; s++) {
		status[s] = W5100.readSnSR(s);
		if (status[s] == SnSR::CLOSED) goto makesocket;
	}
	//Serial.printf("W5000socket step2\n");
	// as a last resort, forcibly close any already closing
	/*	for (s=0; s < maxindex; s++) {			// commented out Feb 1 2019. Sockets in these states should be allowed to complete close process
	uint8_t stat = status[s];
	if (stat == SnSR::LAST_ACK) goto closemakesocket;
	if (stat == SnSR::TIME_WAIT) goto closemakesocket;
	if (stat == SnSR::FIN_WAIT) goto closemakesocket;
	if (stat == SnSR::CLOSING) goto closemakesocket;
	} 
#if 0																	// enabled Jan 30, 2019, disabled in favor of manageSockets() function
	DEBUG_PRINT("W5000socket step3\n");
	// next, use any that are effectively closed
	for (s=0; s < MAX_SOCK_NUM; s++) {
		uint8_t stat = status[s];
		// TODO: this also needs to check if no more data
		if (stat == SnSR::CLOSE_WAIT && socketRecvAvailable(s) == 0) goto closemakesocket;  // added socketRecAvailable(s) check on Jan 30, 2019
	}
#endif */
	SPI.endTransaction();
	//DEBUG_PRINTLN("****************************** socketBegin - ALL SOCKETS ARE IN USE ******************************");
	return MAX_SOCK_NUM; // all sockets are in use
/*closemakesocket:
	//Serial.printf("W5000socket close\n");
	DEBUG_PRINT("socketBegin() - closing socket #");
	DEBUG_PRINTLN(s);
	W5100.execCmdSn(s, Sock_CLOSE);
	if (!W5100.waitForCmd(s, SnSR::CLOSED)) {
		DEBUG_PRINTLN("socketBegin() - timeout waiting for socket to close");
	}*/
makesocket:
	//Serial.printf("W5000socket %d\n", s);
	EthernetServer::server_port[s] = 0;
	//delayMicroseconds(250); // TODO: is this needed??  -rs 11Feb2019
	W5100.writeSnMR(s, protocol);
	W5100.writeSnIR(s, 0xFF);
	if (port > 0) {
		W5100.writeSnPORT(s, port);
	} else {
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
	/*	else {
	DEBUG_PRINT("socketBegin() - opened socket #");
	DEBUG_PRINT(s);
	if (protocol == SnMR::UDP)
	DEBUG_PRINTLN(" for UDP connection");
	else
	DEBUG_PRINTLN(" for TCP connection");
	}

	if (protocol == SnMR::TCP) {
		uint8_t xxx = W5100.readSnSR(s);
		while (xxx != SnSR::INIT) 		// Wait for Sock_OPEN command to complete
			xxx = W5100.readSnSR(s);
	} */

	state[s].RX_RSR = 0;
	state[s].RX_RD  = W5100.readSnRX_RD(s); // always zero?
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
	uint8_t s, status[MAX_SOCK_NUM], chip, maxindex=MAX_SOCK_NUM;

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return MAX_SOCK_NUM; // immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4; // W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	// look at all the hardware sockets, use any that are closed (unused)
	for (s=0; s < maxindex; s++) {
		status[s] = W5100.readSnSR(s);
		if (status[s] == SnSR::CLOSED) goto makesocket;
	}
	//Serial.printf("W5000socket step2\n");
	// as a last resort, forcibly close any already closing
#if 0   // +rs 2/8/2019
	for (s=0; s < maxindex; s++) {
		uint8_t stat = status[s];
		if (stat == SnSR::LAST_ACK) goto closemakesocket;
		if (stat == SnSR::TIME_WAIT) goto closemakesocket;
		if (stat == SnSR::FIN_WAIT) goto closemakesocket;
		if (stat == SnSR::CLOSING) goto closemakesocket;
	}

	Serial.printf("W5000socket step3\n");
	// next, use any that are effectively closed
	for (s=0; s < MAX_SOCK_NUM; s++) {
		uint8_t stat = status[s];
		// TODO: this also needs to check if no more data
		if (stat == SnSR::CLOSE_WAIT) goto closemakesocket;
	}
#endif
	SPI.endTransaction();
	return MAX_SOCK_NUM; // all sockets are in use
closemakesocket:
	//Serial.printf("W5000socket close\n");
	W5100.execCmdSn(s, Sock_CLOSE);
makesocket:
	//Serial.printf("W5000socket %d\n", s);
	EthernetServer::server_port[s] = 0;
	delayMicroseconds(250); // TODO: is this needed??
	W5100.writeSnMR(s, protocol);
	W5100.writeSnIR(s, 0xFF);
	if (port > 0) {
		W5100.writeSnPORT(s, port);
	} else {
		// if don't set the source port, set local_port number.
		if (++local_port < 49152) local_port = 49152;
		W5100.writeSnPORT(s, local_port);
	}
	// Calculate MAC address from Multicast IP Address
	byte mac[] = {  0x01, 0x00, 0x5E, 0x00, 0x00, 0x00 };
	mac[3] = ip[1] & 0x7F;
	mac[4] = ip[2];
	mac[5] = ip[3];
	W5100.writeSnDIPR(s, ip.raw_address());   //239.255.0.1
	W5100.writeSnDPORT(s, port);
	W5100.writeSnDHAR(s, mac);
	W5100.execCmdSn(s, Sock_OPEN);
	state[s].RX_RSR = 0;
	state[s].RX_RD  = W5100.readSnRX_RD(s); // always zero?
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
	DEBUG_PRINT("socketClose() - closing socket #");
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.execCmdSn(s, Sock_CLOSE);
	SPI.endTransaction();
	DEBUG_PRINT("socketClose() - closing socket #");
	DEBUG_PRINTLN(s);
	if (!W5100.waitForCmd(s, SnSR::CLOSE_WAIT, SnSR::CLOSED, SnSR::FIN_WAIT)) {
		DEBUG_PRINTLN("socketClose() - timeout waiting for socket to close");
	} else {
		DEBUG_PRINT("socketClose() - Socket #");
		DEBUG_PRINT(s);
		DEBUG_PRINTLN(" closed");
	}
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
void EthernetClass::socketConnect(uint8_t s, uint8_t * addr, uint16_t port)
{
	// set destination IP
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	W5100.writeSnDIPR(s, addr);
	W5100.writeSnDPORT(s, port);
	delay(90);				// +rs 1/10/2019 
	W5100.execCmdSn(s, Sock_CONNECT);
	SPI.endTransaction();
	if (!W5100.waitForCmd(s, SnSR::ESTABLISHED)) {
		DEBUG_PRINTLN("socketConnect() - timeout waiting for socket to connect");
	}
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
	} else {
		DEBUG_PRINT("socketDisconnect() - Socket #");
		DEBUG_PRINT(s);
		DEBUG_PRINTLN(" disconnected");
	}
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

static void read_data(uint8_t s, uint16_t src, uint8_t *dst, uint16_t len)
{
	uint16_t size;
	uint16_t src_mask;
	uint16_t src_ptr;

	//Serial.printf("read_data, len=%d, at:%d\n", len, src);
	src_mask = (uint16_t)src & W5100.SMASK;
	src_ptr = W5100.RBASE(s) + src_mask;

	if (W5100.hasOffsetAddressMapping() || src_mask + len <= W5100.SSIZE) {
		W5100.read(src_ptr, dst, len);
	} else {
		size = W5100.SSIZE - src_mask;
		W5100.read(src_ptr, dst, size);
		dst += size;
		W5100.read(W5100.RBASE(s), dst, len - size);
	}
}

// Receive data.  Returns size, or -1 for no data, or 0 if connection closed
//
int EthernetClass::socketRecv(uint8_t s, uint8_t *buf, int16_t len)
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
		if ( status == SnSR::LISTEN || status == SnSR::CLOSED ||
			status == SnSR::CLOSE_WAIT ) {
				/////////////////////// Should probably add FIN_WAIT, CLOSING and LAST_ACK to this list of statuses indicating socket is closing
				// The remote end has closed its side of the connection,
				// so this is the eof state.  Ditto for TIME_Wait +rs
				ret = 0;
		} else {
			// The connection is still up, but there's no data waiting to be read
			ret = -1;
		}
	} else {
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
		} else {
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


static void write_data(uint8_t s, uint16_t data_offset, const uint8_t *data, uint16_t len)
{
	uint16_t ptr = W5100.readSnTX_WR(s);
	ptr += data_offset;
	uint16_t offset = ptr & W5100.SMASK;
	uint16_t dstAddr = offset + W5100.SBASE(s);

	if (W5100.hasOffsetAddressMapping() || offset + len <= W5100.SSIZE) {
		W5100.write(dstAddr, data, len);
	} else {
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
uint16_t EthernetClass::socketSend(uint8_t s, const uint8_t * buf, uint16_t len)
{
	uint8_t status=0;
	uint16_t ret=0;
	uint16_t freesize=0;

	if (len > W5100.SSIZE) {
		ret = W5100.SSIZE; // check size not to exceed MAX size.
	} else {
		ret = len;
	}

	// if freebuf is available, start.
	uint32_t start = millis();										// record time loop is entered +rs 17Feb2019
	do {
		SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
		freesize = getSnTX_FSR(s);
		status = W5100.readSnSR(s);
		SPI.endTransaction();
		if ((status != SnSR::ESTABLISHED) && (status != SnSR::CLOSE_WAIT)) {
			ret = 0;
			break;
		}
		if (millis() - start > 1000) {							// check for timeout +rs 17Feb2019 - find a way to use user-configurable _timeout
			ret = 0;
			return ret;
		}
		yield();
	} while (freesize < ret);				// this would be a good place to add a timeout in case peer process crashes without closing socket

	// copy data
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	write_data(s, 0, (uint8_t *)buf, ret);
	W5100.execCmdSn(s, Sock_SEND);

	/* +2008.01 bj */
//	while ( (W5100.readSnIR(s) & SnIR::SEND_OK) != SnIR::SEND_OK ) {
		/* m2008.01 [bj] : reduce code */
/*		if ( W5100.readSnSR(s) == SnSR::CLOSED ) {
			SPI.endTransaction();
			return 0;
		}
		SPI.endTransaction();
		yield();
		SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	} */
	/* +2008.01 bj */
	//W5100.writeSnIR(s, SnIR::SEND_OK);
	SPI.endTransaction();
	EthernetClass::lastSocketUse[s] = millis();			// record time at which socket sent data to support socket management.
	return ret;
}

uint16_t EthernetClass::socketSendAvailable(uint8_t s)
{
	uint8_t status=0;
	uint16_t freesize=0;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	freesize = getSnTX_FSR(s);
	status = W5100.readSnSR(s);
	SPI.endTransaction();
	if ((status == SnSR::ESTABLISHED) || (status == SnSR::CLOSE_WAIT)) {
		return freesize;
	}
	return 0;
}

uint16_t EthernetClass::socketBufferData(uint8_t s, uint16_t offset, const uint8_t* buf, uint16_t len)
{
	//Serial.printf("  bufferData, offset=%d, len=%d\n", offset, len);
	uint16_t ret =0;
	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);
	uint16_t txfree = getSnTX_FSR(s);
	if (len > txfree) {
		ret = txfree; // check size not to exceed MAX size.
	} else {
		ret = len;
	}
	write_data(s, offset, buf, ret);
	SPI.endTransaction();
	return ret;
}

bool EthernetClass::socketStartUDP(uint8_t s, uint8_t* addr, uint16_t port)
{
	if ( ((addr[0] == 0x00) && (addr[1] == 0x00) && (addr[2] == 0x00) && (addr[3] == 0x00)) ||
		((port == 0x00)) ) {
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
	while ( (W5100.readSnIR(s) & SnIR::SEND_OK) != SnIR::SEND_OK ) {
		if (W5100.readSnIR(s) & SnIR::TIMEOUT) {
			/* +2008.01 [bj]: clear interrupt */
			W5100.writeSnIR(s, (SnIR::SEND_OK|SnIR::TIMEOUT));
			SPI.endTransaction();
			//Serial.printf("sendUDP timeout\n");
			return false;
		}
		SPI.endTransaction();
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

	uint8_t chip;							// type of Wiznet chip in system
	uint8_t disconnectableSockets = 0;		// number of sockets in a condition that allows them to be safely disconnected
	uint8_t established = 0;				// number of sockets in ESTABLISHED state (other than LANclient socket)
	uint8_t listening = 0;					// number of sockets in LISTEN state
	uint32_t maxAge;						// the 'age' of the socket in a 'disconnectable' state that was last used the longest time ago
	uint8_t maxindex = MAX_SOCK_NUM;		// the number of sockets in use on the Wiznet chip
	uint32_t minForceClosedAge;				// minimum age at which idle socket will be forced to close
	uint8_t oldest;							// the socket number of the 'oldest' disconnectable socket
	uint8_t s;								// loop counter used to index through sockets
	uint32_t socketAge[MAX_SOCK_NUM];		// array storing the age of sockets in a 'disconnectable' state
	int8_t socketsAvailable = 0;			// number of sockets available for use (i.e., in a CLOSED state)
	int8_t socketsDisconnecting = 0;		// number of sockets in the process of disconnecting, either upon entry or due to commands sent by this function
	uint8_t status;							// value of some socket's Sn_SR
	uint8_t toDisconnect;					// number of sockets that will be sent DISCON commands

	// first check hardware compatibility
	chip = W5100.getChip();
	if (!chip) return;															// immediate error if no hardware detected
#if MAX_SOCK_NUM > 4
	if (chip == 51) maxindex = 4;												// W5100 chip never supports more than 4 sockets
#endif
	//Serial.printf("W5000socket begin, protocol=%d, port=%d\n", protocol, port);

	// initialize the socketAge array to a state that indicates no "ESTABLISHED" sockets are available to disconnect
	for (s = 0; s < maxindex; s++)
		socketAge[s] = 0;

	SPI.beginTransaction(SPI_ETHERNET_SETTINGS);								// begin SPI transaction
	// look at all the hardware sockets, record and take action based on current states
	for (s = 0; s < maxindex; s++) {											// for each hardware socket ...
		status = W5100.readSnSR(s);												//  get socket status...
		if (status == SnSR::CLOSE_WAIT && socketRecvAvailable(s) == 0) {		//   if CLOSE_WAIT and no data available to read...
 			W5100.execCmdSn(s, Sock_DISCON);								    //       send DISCON command...
			EthernetClass::lastSocketUse[s] = millis();							//        record time at which it was sent so we can send CLOSE command if peer doesn't respond...
			socketsDisconnecting++;												//         and increment the number of sockets that are disconnecting.
			DEBUG_PRINT("EthernetClass::manageSockets() - DISCON command sent for socket #");
			DEBUG_PRINTLN(s);
		} else if (status == SnSR::CLOSED) {									//   else if closed socket...
			socketsAvailable++;													//    increment available sockets...
		} else if (status == SnSR::FIN_WAIT || status == SnSR::CLOSING ||		//   else if socket is in the process of closing...
			status == SnSR::TIME_WAIT || status == SnSR::LAST_ACK) {
			socketsDisconnecting++;												//    increment the number of sockets that are disconnecting...
			if (millis() - EthernetClass::lastSocketUse[s] > 250) {				//     if it's been more than 250 milliseconds since disconnect command was sent...
				W5100.execCmdSn(s, Sock_CLOSE);									//	    send CLOSE command...
				EthernetClass::lastSocketUse[s] = millis();						//       and record time at which it was sent so we don't do it repeatedly.
				DEBUG_PRINT("EthernetClass::manageSockets() - CLOSE command sent for socket #");
				DEBUG_PRINTLN(s);
			}
		}
		else if (status == SnSR::ESTABLISHED && s != doNotDisconnectSocket && 	//   else if socket is connected and caller hasn't requested to keep it that way...
				 socketRecvAvailable(s) == 0) {									//    and socket has no data waiting to be read...
			established++;														//	    count it...
			socketAge[s] = millis() - EthernetClass::lastSocketUse[s];			//       and record time since last socket use.
		}
		else if (status == SnSR::LISTEN) {										//   else if socket is in LISTEN state...
			listening++;														//    count it.
		}
	}

	// begin listening on all CLOSED sockets but one, leaving one for rapid LANclient connection changes and NTP updates
	while ((socketsAvailable > 1 && listening < maxListeners) ||				//  while more than 1 socket is in CLOSED state and less than maxListeners are in LISTEN state...
		   (socketsAvailable > 0 && listening == 0)) {							//   OR at least ONE socket is available and NO sockets are listening...
		if (server != NULL) {													//     if server parameter isn't NULL...
			if (server->begin2()) {												//     start listening on a CLOSED socket...
				socketsAvailable--;												//      if successful, decrement number of available sockets...
				listening++;													//       and increment number of listeners.
			} else {
				break;															//      else break to avoid infinite loop.
			}
		} else {
			break;
		}
	}

	// Now disconnect idle sockets, starting with the one that's been idle the longest
	if (listening == 1 && socketsAvailable == 0 && socketsDisconnecting == 0)	// if only 1 socket is listening and no sockets are free or disconnecting...
		minForceClosedAge = 5000;												//  force 'excess' sockets that have been idle at least 5 seconds to disconnect...
	else																		// otherwise...
		minForceClosedAge = 30000;												//  force 'excess' sockets that have been idle at least 30 seconds to disconnect...

	if (established > 2) {
		toDisconnect = established - 2;											// set "toDisconnect" so no more than 2 idle sockets are left connected

		while (toDisconnect > 0) {												// while additional sockets are needed and eligible sockets remain...

			oldest = MAX_SOCK_NUM;												//  find the 'oldest' disconnectable socket...
			maxAge = 0;
			for (s = 0; s < maxindex; s++) {									//   by scanning the socketAge array populated above...
				if (socketAge[s] > maxAge) {									//    if current socket's "age" is greater than the max age recorded so far...
					oldest = s;													//     record the socket number...
					maxAge = socketAge[s];										//      and make its age the new max age.
				}
			}

			if (oldest != MAX_SOCK_NUM && maxAge > minForceClosedAge) {			// if an "oldest" socket is found that's been idle at least minForceClosedAge milliseconds...
				W5100.execCmdSn(oldest, Sock_DISCON);							//  send DISCON command...
				EthernetClass::lastSocketUse[oldest] = millis();				//   record time at which it was sent...
				socketAge[oldest] = 0;											//    mark the socket as no longer available to be disconnected...
				toDisconnect--;													//     and decrement the number of sockets remaining to disconnect.
				DEBUG_PRINTLN("EthernetClass::manageSockets() - DISCON command sent for socket #");
				DEBUG_PRINT("EthernetClass::manageSockets() - DISCON command sent for socket #");
				DEBUG_PRINTLN(oldest);
			} else {															// otherwise...
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
