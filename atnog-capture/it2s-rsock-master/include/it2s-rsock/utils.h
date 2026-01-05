#ifndef UTILS_H
#define UTILS_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/if_packet.h>
#include <linux/sockios.h>
#include <net/if.h>
#include <netinet/ether.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>

/*
 * IT2S-GN defines a maximum packet size equal to 2360 bytes.
 */
#define IT2S_RSOCK_BUFF_SIZE 2360

/*
 * Creates ifreq struct from interface name.
 *
 * interface: String with interface name.
 * returns: ifreq struct.
 */
struct ifreq it2s_rsock_create_ifreq(unsigned char* interface);

/*
 * Reads interface index to the ifreq struct, and binds socket to the interface.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_index(int *raw_sock, struct ifreq *raw_int, uint16_t ether_type);

/*
 * Sets interface to promiscuous mode. 
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_prom(int *raw_sock, struct ifreq *raw_int);

/*
 * Reads interface address to the ifreq struct.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_mac(int *raw_sock, struct ifreq *raw_int);

/*
 * Creates transmission socket.
 *
 * returns: socket.
 */
int it2s_rsock_create_tsocket(uint16_t ether_type);

/*
 * Creates listening socket.
 *
 * returns: socket.
 */
int it2s_rsock_create_lsocket(uint16_t ether_type);

/*
 * Creates ethernet header with broadcast destination.
 *
 * raw_int: pointer to the ifreq.
 * returns: frame header.
 */
unsigned char* it2s_rsock_create_dummy_frame(struct ifreq *raw_int);

/*
 * Given a buffer with a ethernet header, adds payload.
 *
 * raw_packet_buff: pointer to the ethernet header.
 * raw_ptotal_len: header size.
 * data: pointer to the payload.
 * data_len: payload size.
 * returns: pointer to the frame.
 */
unsigned char* it2s_rsock_set_data(unsigned char* raw_packetbuff, int raw_ptotal_len, unsigned char* data, int data_len);

/*
 * Sends frame through interface.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * raw_packetbuff: pointer to the frame.
 * raw_ptotal_len: frame size.
 * returns: error code.
 */
int it2s_rsock_send_frame(int *raw_sock, struct ifreq *raw_int, unsigned char *raw_packetbuff, int raw_ptotal_len);

/*
 * Listen for a frame.
 *
 * raw_sock: pointer to the socket.
 * buf: pointer to the buffer that will contain the received frame.
 * addr: the ego address
 * frame_len: size of the received frame.
 * returns: error code.
 */
//int raw_receive_frame(int *raw_sock, struct ifreq *raw_int, unsigned char* frame, int* frame_len);
int it2s_rsock_receive_frame(struct pollfd* fds, int n_fds, uint8_t* addr, unsigned char* buf, int* frame_len);

/*
 * Closes a socket.
 *
 * raw_sock: pointer to the socket.
 * returns: error code.
 */
int it2s_rsock_close_socket(int *raw_sock);

int it2s_rsock_get_address(int sock, struct ifreq* raw_int, uint8_t* address);

#endif
