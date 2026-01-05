#include "it2s-rsock/utils.h"
#include <poll.h>

/**
 * Creates ifreq struct from interface name.
 *
 * interface: String with interface name.
 * returns: ifreq struct.
 */
struct ifreq it2s_rsock_create_ifreq(unsigned char* interface){
    struct ifreq raw_int;
    memset(&raw_int, 0, sizeof(raw_int));
    strncpy(raw_int.ifr_name, interface, IFNAMSIZ-1);
    return raw_int;
}

/**
 * Reads interface index to the ifreq struct.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_index(int *raw_sock, struct ifreq *raw_int, uint16_t ether_type){
    if(ioctl(*raw_sock, SIOCGIFINDEX, raw_int) < 0){
        printf("error in SIOCGIFINDEX ioctl reading\n"); 
        return 0;
    }
    struct sockaddr_ll addr = {0};
    addr.sll_family = AF_PACKET;
    addr.sll_ifindex = raw_int->ifr_ifindex;
    addr.sll_protocol = htons(ether_type);
    if (bind(*raw_sock, (struct sockaddr*)&addr, sizeof(addr)) == -1){
        printf("interface bind error\n");
        return 0;
    }
    return 1;
}

/**
 * Reads interface address to the ifreq struct.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_mac(int *raw_sock, struct ifreq *raw_int){
    if(ioctl(*raw_sock, SIOCGIFHWADDR, raw_int) < 0){
        printf("error in SIOCGIFHWADDR ioctl reading\n"); 
        return 0;
    }
    return 1;
}

/**
 * Sets interface to promiscuous mode. 
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * returns: error code.
 */
int it2s_rsock_set_int_prom(int *raw_sock, struct ifreq *raw_int){
    struct packet_mreq mreq = {0};
    mreq.mr_ifindex = raw_int->ifr_ifindex;
    mreq.mr_type = PACKET_MR_PROMISC;
    setsockopt(*raw_sock, SOL_PACKET, PACKET_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
    return 1;
}

/**
 * Creates transmission socket.
 *
 * returns: socket.
 */
int it2s_rsock_create_tsocket(uint16_t ethertype){
    int raw_sock = socket(AF_PACKET, SOCK_RAW, htons(ethertype));
    if (raw_sock == -1){
        printf("socket creation error\n");
        return 0;
    }
//#ifdef DEBUG
    static const int32_t sock_qdisc_bypass = 1;
    int32_t sock_qdisc_ret = setsockopt(raw_sock, SOL_PACKET, PACKET_QDISC_BYPASS, &sock_qdisc_bypass, sizeof(sock_qdisc_bypass));
    if(sock_qdisc_ret == -1){
        printf("error setting qdisc bypass");
        return 0;
    }
//#endif
    return raw_sock;
}

/**
 * Creates listening socket.
 *
 * returns: socket.
 */
int it2s_rsock_create_lsocket(uint16_t ether_type){
    int raw_lsock = socket(PF_PACKET, SOCK_RAW, htons(ether_type));
    if (raw_lsock == -1){
        printf("lsocket creation error\n");
        return 0;
    }
    return raw_lsock;
}


/**
 * Creates ethernet header with broadcast destination.
 *
 * raw_int: pointer to the ifreq.
 * returns: frame header.
 */
unsigned char* it2s_rsock_create_dummy_frame(struct ifreq *raw_int){
    unsigned char* raw_packetbuff = (unsigned char*) malloc(IT2S_RSOCK_BUFF_SIZE);
    memset(raw_packetbuff, 0, IT2S_RSOCK_BUFF_SIZE);
    struct ethhdr *raw_ethdr = (struct ethhdr *)(raw_packetbuff);
    raw_ethdr->h_source[0] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[0]);
    raw_ethdr->h_source[1] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[1]);
    raw_ethdr->h_source[2] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[2]);
    raw_ethdr->h_source[3] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[3]);
    raw_ethdr->h_source[4] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[4]);
    raw_ethdr->h_source[5] = (uint8_t)(raw_int->ifr_hwaddr.sa_data[5]);
    raw_ethdr->h_dest[0] = 0xFF;
    raw_ethdr->h_dest[1] = 0xFF;
    raw_ethdr->h_dest[2] = 0xFF;
    raw_ethdr->h_dest[3] = 0xFF;
    raw_ethdr->h_dest[4] = 0xFF;
    raw_ethdr->h_dest[5] = 0xFF;
    raw_ethdr->h_proto = htons(ETH_P_IP);
    return raw_packetbuff;
}

/**
 * Given a buffer with a ethernet header, adds payload.
 *
 * raw_packet_buff: pointer to the ethernet header.
 * raw_ptotal_len: header size.
 * data: pointer to the payload.
 * data_len: payload size.
 * returns: pointer to the frame.
 */
unsigned char* it2s_rsock_set_data(unsigned char* raw_packetbuff, int raw_ptotal_len, unsigned char* data, int data_len){
    for(int i = 0; i < data_len; i++){
        raw_packetbuff[raw_ptotal_len++] = data[i];
    }
    return raw_packetbuff;
}

/**
 * Sends frame through interface.
 *
 * raw_sock: pointer to the socket.
 * raw_int: pointer to the ifreq.
 * raw_packetbuff: pointer to the frame.
 * raw_ptotal_len: frame size.
 * returns: error code.
 */
int it2s_rsock_send_frame(int *raw_sock, struct ifreq *raw_int, unsigned char *raw_packetbuff, int raw_ptotal_len){
    struct sockaddr_ll raw_addr;
    raw_addr.sll_ifindex = raw_int->ifr_ifindex;
    raw_addr.sll_halen = ETH_ALEN; 
    raw_addr.sll_addr[0] = 0xFF;
    raw_addr.sll_addr[1] = 0xFF;
    raw_addr.sll_addr[2] = 0xFF;
    raw_addr.sll_addr[3] = 0xFF;
    raw_addr.sll_addr[4] = 0xFF;
    raw_addr.sll_addr[5] = 0xFF;
    int send_len = sendto(*raw_sock, raw_packetbuff, raw_ptotal_len, 0, (const struct sockaddr*)&raw_addr, sizeof(struct sockaddr_ll));
    if(send_len < 0){
        printf("error sending packet\n");
        return 0;
    }
    return 1;
}

/**
 * Listen for a frame.
 *
 * buf: pointer to the buffer that will contain the received frame.
 * frame_len: size of the received frame.
 * returns: error code.
 */
int it2s_rsock_receive_frame(struct pollfd* fds, int n_fds, uint8_t* addr, unsigned char* buf, int* frame_len){
    struct sockaddr saddr;
    int saddr_len = sizeof(saddr);
    int ret = 1;
    poll(fds, n_fds, 1000);
    for (int i = 0; i < n_fds; ++i) {
        if (fds[i].revents) {
            *frame_len = recvfrom(fds[i].fd, buf, IT2S_RSOCK_BUFF_SIZE, 0, &saddr, (socklen_t*) &saddr_len);
            break;
        }
    }

    struct ethhdr *eh = (struct ethhdr *) buf;
    if (*frame_len == -1){
        printf("Error %d: %s (rv: %d)\n", errno, strerror(errno), *frame_len);
        ret = 0;
    } else if (
            eh->h_dest[0] != 0xFF || 
            eh->h_dest[1] != 0xFF || 
            eh->h_dest[2] != 0xFF || 
            eh->h_dest[3] != 0xFF || 
            eh->h_dest[4] != 0xFF || 
            eh->h_dest[5] != 0xFF) {
        ret = 0;
    }

    if(memcmp(eh->h_source, addr, 6) == 0){
	    ret = 0;
    } 


    return ret;
}

int it2s_rsock_get_address(int sock, struct ifreq* raw_int, uint8_t* address) {

    ioctl(sock, SIOCGIFHWADDR, raw_int);
    memcpy(address, raw_int->ifr_addr.sa_data, 6);

    return 0;
}

/**
 * Closes a socket.
 *
 * raw_sock: pointer to the socket.
 * returns: error code.
 */
int it2s_rsock_close_socket(int *raw_sock){
    close(*raw_sock); 
    return 1;
}
