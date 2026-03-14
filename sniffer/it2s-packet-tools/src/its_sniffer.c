#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <it2s-rsock/rsock.h>
#include <it2s-rsock/utils.h>
#include <it2s-ublox/ublox.h>
#include <it2s-llc.h>
#include <it2s-mac.h>
#include <it2s-gn/gn.h>
#include <it2s-asn/etsi-its-v2/cam/EI2_CAM.h>
#include <it2s-asn/etsi-its-v2/denm/EI2_DENM.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/uper_decoder.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/jer_encoder.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/EI2_MessageId.h>
#include "its_common.h"
#include "logger.h"

/* Adapt as needed */
void cam_cb(EI2_CAM_t* cam) {
    int json_buffer_len = 2048;
    char* json_buffer = calloc(json_buffer_len, 1);
    
    log_info("[cam] latitude: %d", cam->cam.camParameters.basicContainer.referencePosition.latitude);
    log_info("[cam] longitude: %d", cam->cam.camParameters.basicContainer.referencePosition.longitude);
    if (cam->cam.camParameters.highFrequencyContainer.present == EI2_HighFrequencyContainer_PR_basicVehicleContainerHighFrequency) {
        EI2_BasicVehicleContainerHighFrequency_t* bvc_hf = &cam->cam.camParameters.highFrequencyContainer.choice.basicVehicleContainerHighFrequency;
        log_info("[cam] speed: %d", bvc_hf->speed.speedValue);
    }

    asn_enc_rval_t enc = asn_encode_to_buffer(NULL, ATS_JER_MINIFIED, &asn_DEF_EI2_CAM, cam, json_buffer, json_buffer_len);
    if (enc.encoded == -1) {
        log_error("[cam] error encoding CAM to JSON");
        return;
    }
    log_info("[cam] JSON encoded (size: %d): %s", enc.encoded, json_buffer);
    free(json_buffer);
}

/* Adapt as needed */
void denm_cb(EI2_DENM_t* denm) {
    int json_buffer_len = 2048;
    char* json_buffer = calloc(json_buffer_len, 1);
    asn_enc_rval_t enc = asn_encode_to_buffer(NULL, ATS_JER_MINIFIED, &asn_DEF_EI2_DENM, denm, json_buffer, json_buffer_len);
    if (enc.encoded == -1) {
        log_error("[denm] error encoding DENM to JSON");
        return;
    }
    log_info("[denm] JSON encoded (size: %d): %s", enc.encoded, json_buffer);
    free(json_buffer);
}

void generic_cb(int tx_access, uint8_t* packet, size_t packet_len) {
    int rv;

    /* Print raw packet hex */
    char *rx_hex = malloc(packet_len * 2 + 1);
    if (!rx_hex) return;
    char *buf_ptr = rx_hex;
    for (int i = 0; i < packet_len; i++)
        buf_ptr += sprintf(buf_ptr, "%02x", packet[i]);
    log_info("[phy] <- received packet | size: %dB data: %s", packet_len, rx_hex);
    free(rx_hex);

    uint8_t bufA[IT2S_WIFI_MAX_PACKET_SIZE];
    uint8_t bufB[IT2S_WIFI_MAX_PACKET_SIZE];
    uint32_t lenA = 0, lenB = 0;
    uint8_t src_mac[6], dst_mac[6];
    uint16_t ether_type;
    uint8_t *gn_buf;
    uint32_t gn_len;

    /* rsock: plain 802.3 ethernet frame - dst_mac(6) | src_mac(6) | ethertype BE(2) | GN payload */
    if (tx_access == ACCESS_RSOCK) {
        if (packet_len < 14) {
            log_error("[phy] packet too short");
            return;
        }
        memcpy(dst_mac, packet, 6);
        memcpy(src_mac, packet + 6, 6);
        ether_type = ((uint16_t)packet[12] << 8) | packet[13];
        gn_buf = packet + 14;
        gn_len = (uint32_t)packet_len - 14;
        log_debug("[mac] decap ok | src: %02x:%02x:%02x:%02x:%02x:%02x dst: %02x:%02x:%02x:%02x:%02x:%02x ether_type: 0x%04x len: %uB",
            src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5],
            dst_mac[0], dst_mac[1], dst_mac[2], dst_mac[3], dst_mac[4], dst_mac[5],
            ether_type, gn_len);
    } 
    
    /* ublox: full 802.11 frame - MAC decap + LLC decap + FCS strip */
    else if (tx_access == ACCESS_UBLOX) {
        /* Decap MAC */
        rv = it2s_mac_decap(packet, (uint16_t)packet_len, bufA, &lenA, src_mac, dst_mac);
        if (rv) {
            log_error("[mac] decap failed");
            return;
        }
        log_debug("[mac] decap ok | len: %uB src: %02x:%02x:%02x:%02x:%02x:%02x dst: %02x:%02x:%02x:%02x:%02x:%02x",
            lenA,
            src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5],
            dst_mac[0], dst_mac[1], dst_mac[2], dst_mac[3], dst_mac[4], dst_mac[5]);

        /* Decap LLC */
        rv = it2s_llc_decap(bufA, (uint16_t)lenA, bufB, &lenB, &ether_type);
        if (rv) {
            log_error("[llc] decap failed");
            return;
        }
        log_debug("[llc] decap ok | ether_type: 0x%04x len: %uB", ether_type, lenB);

        /* Remove FCS (4 bytes at end of MAC frame) */
        if (lenB < 4) {
            log_error("[fcs] payload too short to strip FCS");
            return;
        }
        lenB -= 4;
        gn_buf = bufB;
        gn_len = lenB;
    }

    /* Decap GeoNetworking (Basic Header + Common Header + Extended Header) */
    /* Peek at BH NH field):
     *   0x01 = Common Header (unsecured)
     *   0x02 = Secured Packet */
    if (gn_len >= 1 && (gn_buf[0] & M_BNH) == 0x02) {
        log_warn("[gn] secured packet detected - security decap not supported, skipping");
        return;
    }
    it2s_gn_header_t *gn = it2s_gn_header_new(0, 0);
    if (!gn) {
        log_error("[gn] header allocation failed");
        return;
    }
    rv = it2s_gn_decap(gn_buf, gn_len, bufA, &lenA, gn);
    if (rv) {
        log_error("[gn] decap failed");
        it2s_gn_header_free(gn);
        return;
    }
    log_debug("[gn] decap ok | ht: %d hst: %d len: %uB", gn->ch.ht, gn->ch.hst, lenA);
    it2s_gn_header_free(gn);

    /* Decap BTP-B header: dst port (2B) + dst port info (2B) */
    if (lenA < L_BTP_B) {
        log_error("[btp] decap failed | payload too short");
        return;
    }
    uint16_t btp_dst_port = (bufA[0] << 8) | bufA[1];
    log_debug("[btp] decap ok | dst port: %u", btp_dst_port);

    uint8_t *its_payload = bufA + L_BTP_B;
    uint32_t its_len = lenA - L_BTP_B;

    /* Select ITS message descriptor based on BTP destination port */
    asn_TYPE_descriptor_t *its_msg_descriptor = NULL;
    void *its_msg = NULL;
    switch (btp_dst_port) {
        case BTP_PORT_CAM: {
            its_msg_descriptor = &asn_DEF_EI2_CAM;
            its_msg = calloc(1, sizeof(EI2_CAM_t));
            break;
        }
        case BTP_PORT_DENM: {
            its_msg_descriptor = &asn_DEF_EI2_DENM;
            its_msg = calloc(1, sizeof(EI2_DENM_t));
            break;
        }
        default:
            log_warn("[btp] unsupported BTP port: %u, skipping...", btp_dst_port);
            return;
    }

    if (!its_msg) {
        log_error("[its] allocation failed");
        return;
    }

    /* UPER decode ITS message */
    asn_dec_rval_t dec = uper_decode_complete(NULL, its_msg_descriptor, (void **)&its_msg, its_payload, its_len);
    if (dec.code != RC_OK) {
        log_error("[its] UPER decode failed for %s (code: %d)", its_msg_descriptor->name, dec.code);
        ASN_STRUCT_FREE(*its_msg_descriptor, its_msg);
        return;
    }

    /* Process ITS messages */
    switch (btp_dst_port) {
        case BTP_PORT_CAM: {
            EI2_CAM_t* cam = (EI2_CAM_t*) its_msg;
            log_debug("[its] UPER decode ok | cam.stationId: %d]", cam->header.stationId);
            cam_cb(cam);
            break;
        }
        case BTP_PORT_DENM: {
            EI2_DENM_t* denm = (EI2_DENM_t*) its_msg;
            log_debug("[its] UPER decode ok | denm.stationId: %d]", denm->header.stationId);
            denm_cb(denm);
            break;
        }
    }
    ASN_STRUCT_FREE(*its_msg_descriptor, its_msg);
}

void rsock_cb(it2s_rsock_t* rsock, void* obj, unsigned char* packet, int packet_len){
    generic_cb(ACCESS_RSOCK, (uint8_t*) packet, (size_t) packet_len);
}

void ublox_cb(it2s_ublox_t* ublox, void* obj, uint8_t* packet, uint16_t packet_len) {
    generic_cb(ACCESS_UBLOX, (uint8_t*) packet, (size_t) packet_len);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        log_error("missing arguments... usage: its_sniffer <rsock|ublox> <interface>");
        return 1;
    }

    char* access_technology = argv[1];
    char* interface = argv[2];
    pthread_t rx_thread;
    if (!strcmp(access_technology, "rsock")) {
        it2s_rsock_t* rsock = it2s_rsock_init((unsigned char*) interface);
        it2s_rsock_rx_packet_callback_set(rsock, rsock_cb);
        pthread_create(&rx_thread, NULL, (void*(*)(void*)) it2s_rsock_loop, rsock);
    }

    else if (!strcmp(access_technology, "ublox")) {
        it2s_ublox_t* ublox = it2s_ublox_init(UBLOX_CHANNEL, UBLOX_RADIO_C, UBLOX_CHANNEL_CONFIG, UBLOX_ANTENNA, (char*) UBLOX_MCS, UBLOX_POWER);
        it2s_ublox_rx_packet_callback_set(ublox, ublox_cb);
        pthread_create(&rx_thread, NULL, (void*(*)(void*)) it2s_ublox_loop, ublox);
    }

    else {
        log_error("unsupported access technology: %s", access_technology);
        return 1;
    }

    pthread_join(rx_thread, NULL);
    return 0;
}
