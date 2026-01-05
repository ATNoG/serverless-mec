#include "it2s-rsock/rsock.h"
#include "it2s-rsock/utils.h"
#include <stdbool.h>

it2s_rsock_t* it2s_rsock_init(unsigned char* interface) {
    it2s_rsock_t* rsock = calloc(1, sizeof(it2s_rsock_t));

    int lsock = it2s_rsock_create_lsocket(GEONET_ETHERNET_TYPE);
    struct ifreq ifr_p = it2s_rsock_create_ifreq(interface);
    int tsock = it2s_rsock_create_tsocket(GEONET_ETHERNET_TYPE);
    struct ifreq ifr_i = it2s_rsock_create_ifreq(interface);
    rsock->gn.lsock = lsock;
    rsock->gn.ifr_p = ifr_p;
    rsock->gn.tsock = tsock;
    rsock->gn.ifr_i = ifr_i;

    int lsock6 = it2s_rsock_create_lsocket(IPV6_ETHERNET_TYPE);
    struct ifreq ifr_p6= it2s_rsock_create_ifreq(interface);
    int tsock6= it2s_rsock_create_tsocket(IPV6_ETHERNET_TYPE);
    struct ifreq ifr_i6= it2s_rsock_create_ifreq(interface);
    rsock->ipv6.lsock = lsock6;
    rsock->ipv6.ifr_p = ifr_p6;
    rsock->ipv6.tsock = tsock6;
    rsock->ipv6.ifr_i = ifr_i6;

    it2s_rsock_set_int_index(&rsock->gn.lsock, &rsock->gn.ifr_p, GEONET_ETHERNET_TYPE);
    it2s_rsock_set_int_prom(&rsock->gn.lsock, &rsock->gn.ifr_p);
    it2s_rsock_set_int_index(&rsock->ipv6.lsock, &rsock->ipv6.ifr_p, IPV6_ETHERNET_TYPE);
    it2s_rsock_set_int_prom(&rsock->ipv6.lsock, &rsock->ipv6.ifr_p);
    
    it2s_rsock_set_int_index(&rsock->gn.tsock, &rsock->gn.ifr_i, GEONET_ETHERNET_TYPE);
    it2s_rsock_set_int_index(&rsock->ipv6.tsock, &rsock->ipv6.ifr_i, IPV6_ETHERNET_TYPE);

    it2s_rsock_get_address(rsock->gn.lsock, &rsock->gn.ifr_p, rsock->interface_addr);

    rsock->fds[0].fd = rsock->gn.lsock;
    rsock->fds[0].events = POLLIN;
    rsock->fds[1].fd = rsock->ipv6.lsock;
    rsock->fds[1].events = POLLIN;
    rsock->n_fds = 2;


    rsock->Exit = false;

    return rsock;
}

/**
 * Sets user data to rsock struct.
 *
 * rsock: pointer to the rsock struct.
 * obj: pointer to user data object.
 */
void it2s_rsock_user_data_set(it2s_rsock_t* rsock, void* obj){
    rsock->userData = obj;
}

/**
 * Receive a frame.
 *
 * rsock: pointer to rsock struct.
 */
void it2s_rsock_rx_packet(it2s_rsock_t* rsock){
    unsigned char* buf = malloc(IT2S_RSOCK_BUFF_SIZE);
    int frame_len;
    if(it2s_rsock_receive_frame(rsock->fds, rsock->n_fds, rsock->interface_addr, buf, &frame_len))
        rsock->on_packet(rsock, rsock->userData, buf, frame_len);
    free(buf);
}

/**
 * Loop over listening for frames.
 *
 * rsock: pointer to rsock struct.
 * returns: error code.
 */
int it2s_rsock_loop(it2s_rsock_t*rsock){
   while(!rsock->Exit){
      it2s_rsock_rx_packet(rsock);
   }
   return 0;   
}

/**
 * Send a frame.
 *
 * rsock: pointer to rsock struct.
 * config: pointer to config struct.
 * frame: pointer to ethernet frame.
 * frame_len: ethernet frame size.
 * returns: error code.
 */
int it2s_rsock_tx_packet(it2s_rsock_t *rsock, it2s_rsock_config_t *config, unsigned char* frame, int frame_len){

    switch ( htons(*(uint16_t*)(frame+12)) ) {
        case GEONET_ETHERNET_TYPE:
            it2s_rsock_send_frame(&rsock->gn.tsock, &(rsock->gn.ifr_i), frame, frame_len);
            break;
        case IPV6_ETHERNET_TYPE:
            it2s_rsock_send_frame(&rsock->ipv6.tsock, &(rsock->ipv6.ifr_i), frame, frame_len);
            break;
    }
    return 1;
}

/**
 * Sets callback function to rx_packet().
 *
 * rsock: pointer to rsock struct.
 * on_packet: pointer to callback function.
 */
void it2s_rsock_rx_packet_callback_set(it2s_rsock_t *rsock, void (* on_packet)(struct it2s_rsock *, void* obj, unsigned char* frame, int frame_len)){
    rsock->on_packet = on_packet;
}

/**
 * Closes sockets and frees rsock struct memory.
 *
 * rsock: pointer to rsock struct.
 */
void it2s_rsock_free(it2s_rsock_t *rsock){
    it2s_rsock_close_socket(&rsock->gn.lsock);
    it2s_rsock_close_socket(&rsock->gn.tsock);
    it2s_rsock_close_socket(&rsock->ipv6.lsock);
    it2s_rsock_close_socket(&rsock->ipv6.tsock);
    free(rsock);
}
