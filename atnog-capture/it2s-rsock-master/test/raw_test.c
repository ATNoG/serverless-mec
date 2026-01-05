#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <signal.h>
#include "it2s-rsock/rsock.h"
#include "it2s-rsock/utils.h"

void rsock_cb(it2s_rsock_t *rsock, void *obj, unsigned char* packet, int packet_len){
    printf("[rsock] <- received packet");
}

int main() {
    unsigned char* interface = "lo";
    it2s_rsock_t *rsock = malloc(sizeof(struct it2s_rsock));
    int lsock = it2s_rsock_create_lsocket(GEONET_ETHERNET_TYPE);
    struct ifreq ifr_p = it2s_rsock_create_ifreq(interface);
    int tsock = it2s_rsock_create_tsocket(GEONET_ETHERNET_TYPE);
    struct ifreq ifr_i = it2s_rsock_create_ifreq(interface);
    rsock->interface = interface;
    rsock->gn.lsock = lsock;
    rsock->gn.ifr_p = ifr_p;
    rsock->gn.tsock = tsock;
    rsock->gn.ifr_i = ifr_i;

    struct ifreq ifr_m = it2s_rsock_create_ifreq(interface);
    it2s_rsock_set_int_mac(&rsock->gn.tsock, &ifr_m);
    unsigned char *frame = it2s_rsock_create_dummy_frame(&ifr_m);
    int frame_len = sizeof(struct ethhdr);
    unsigned char* data = "data";
    frame = it2s_rsock_set_data(frame, frame_len, data, sizeof(data));
    frame_len += strlen(data);

    if(it2s_rsock_tx_packet(rsock, NULL, frame, frame_len))
        printf("sent packet\n");

    it2s_rsock_rx_packet_callback_set(rsock, rsock_cb);
    it2s_rsock_free(rsock);
}
