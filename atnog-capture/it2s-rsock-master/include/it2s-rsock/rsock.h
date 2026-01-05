#ifndef RSOCK_H
#define RSOCK_H
#include <net/if.h>
#include <stdint.h>
#include <poll.h>

#define GEONET_ETHERNET_TYPE 0x8947
#define IPV6_ETHERNET_TYPE 0x86dd

typedef struct it2s_rsock_config it2s_rsock_config_t;

/*
 * Raw Sockets struct.
 *
 * on_packet: pointer to callback function.
 * interface: pointer to string with interface name.
 * lsock: listening socket.
 * ifr_p: ifreq for setting promiscuous mode.
 * tsock: transmitting socket.
 * ifr_i: ifreq for setting interface index.
 * Exit: used for exiting it2s_rsock_loop().
 * userData: arbitrary data.
 * returns: error code.
 */
typedef struct it2s_rsock {
    void (*on_packet) (struct it2s_rsock *, void *obj, unsigned char* packet, int packet_len);
    unsigned char* interface;
    uint8_t interface_addr[6];

    struct {
        int lsock;
        struct ifreq ifr_p;
        int tsock;
        struct ifreq ifr_i;
    } gn;

    struct {
        int lsock;
        struct ifreq ifr_p;
        int tsock;
        struct ifreq ifr_i;
    } ipv6;

    struct pollfd fds[2];
    int n_fds;

    int Exit;
    void* userData;
} it2s_rsock_t;

typedef struct it2s_rsock_looper {
	it2s_rsock_t rsock;
	uint16_t ether_type;
} it2s_rsock_looper_t;

it2s_rsock_t* it2s_rsock_init(unsigned char* interface);

/*
* Sets user data to rsock struct.
*
* rsock: pointer to the rsock struct.
* obj: pointer to user data object.
*/
void it2s_rsock_user_data_set(it2s_rsock_t* rsock, void* obj);

/*
* Receive a frame.
*
* rsock: pointer to rsock struct.
*/
void it2s_rsock_rx_packet(it2s_rsock_t *rsock);

/*
* Loop over listening for frames.
 *
 * rsock: pointer to rsock struct.
 * returns: error code.
 */
int it2s_rsock_loop(it2s_rsock_t* rsock);

/*
 * Sets callback function to rx_packet().
 *
 * rsock: pointer to rsock struct.
 * on_packet: pointer to callback function.
 */
void it2s_rsock_rx_packet_callback_set(it2s_rsock_t *rsock, void (* on_packet)(struct it2s_rsock*, void *obj, unsigned char* frame, int frame_len));

/*
 * Send a frame.
 *
 * rsock: pointer to rsock struct.
 * config: pointer to config struct.
 * frame: pointer to ethernet frame.
 * frame_len: ethernet frame size.
 * returns: error code.
 */
int it2s_rsock_tx_packet(it2s_rsock_t *rsock, it2s_rsock_config_t *config, unsigned char* frame, int frame_len);

/*
 * Closes sockets and frees rsock struct memory.
 *
 * rsock: pointer to rsock struct.
 */
void it2s_rsock_free(it2s_rsock_t *rsock);

#endif
