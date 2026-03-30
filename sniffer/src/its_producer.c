#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <it2s-rsock/rsock.h>
#include <it2s-rsock/utils.h>
#include <it2s-ublox/ublox.h>
#include <it2s-llc.h>
#include <it2s-mac.h>
#include <it2s-gn/gn.h>
#include <it2s-asn/etsi-its-v2/cam/EI2_CAM.h>
#include <it2s-asn/etsi-its-v2/denm/EI2_DENM.h>
#include "its_common.h"
#include "logger.h"

typedef struct {
    int access_type;
    void* obj;
} producer_ctx_t;

static int encap_btp_gn(uint16_t btp_port, uint8_t *its_payload, uint32_t its_len, uint8_t *out, uint32_t *out_len) {
    int rv;
    uint8_t bufA[IT2S_WIFI_MAX_PACKET_SIZE];
    uint32_t lenA = 0;

    /* BTP-B header: dst port (2B) + dst port info (2B) */
    bufA[0] = (btp_port >> 8) & 0xFF;
    bufA[1] =  btp_port       & 0xFF;
    bufA[2] = 0x00;
    bufA[3] = 0x00;
    memcpy(bufA + L_BTP_B, its_payload, its_len);
    lenA = L_BTP_B + its_len;
    log_debug("[btp] encap ok | len: %uB", lenA);

    /* GN SHB encap */
    it2s_gn_header_t *gn = it2s_gn_header_new(0, EXTENDED_HEADER_SHB);
    if (!gn) {
        log_error("[gn] header allocation failed");
        return -1;
    }

    /* Basic Header */
    gn->bh.version = itsGnProtocolVersion;
    gn->bh.nh = 0x01;
    it2s_gn_calculate_lt(&gn->bh.lt_base, &gn->bh.lt_mult, itsGnDefaultPacketLifetime * 1000);
    gn->bh.rhl = 1;

    /* Common Header */
    gn->ch.nh = 2;
    gn->ch.ht = COMMON_HEADER_HT_TSB;
    gn->ch.hst = COMMON_HEADER_HST_TSB_SINGLE_HOP;
    gn->ch.tc_scf = 0;
    gn->ch.tc_offload = 0;
    gn->ch.tc_id = 0;
    gn->ch.mobile = 1;
    gn->ch.pl = (uint16_t)lenA;
    gn->ch.mhl = itsGnDefaultHopLimit;

    /* Extended Header */
    memset(&gn->eh.choice.shb.source_lpv, 0, sizeof(gn_long_position_vector_t));
    gn->eh.present = EXTENDED_HEADER_SHB;

    rv = it2s_gn_encap(bufA, (uint16_t)lenA, out, out_len, gn);
    it2s_gn_header_free(gn);
    if (rv) {
        log_error("[gn] encap failed");
        return -1;
    }

    log_debug("[gn] encap ok | len: %uB", *out_len);
    return 0;
}

static void tx_packet(producer_ctx_t *ctx, uint8_t *gn_buf, uint32_t gn_len) {
    int rv;
    uint8_t bufA[IT2S_WIFI_MAX_PACKET_SIZE];
    uint8_t bufB[IT2S_WIFI_MAX_PACKET_SIZE];
    uint32_t lenA = 0, lenB = 0;

    /* rsock: plain 802.3 ethernet frame - dst_mac(6) | src_mac(6) | ethertype BE(2) | GN payload */
    if (ctx->access_type == ACCESS_RSOCK) {
        it2s_rsock_t *rsock = (it2s_rsock_t *)ctx->obj;
        uint8_t src_mac[6];
        memcpy(src_mac, rsock->interface_addr, 6);

        /* If interface MAC is all-zeros (e.g. lo), append a 1 */
        int all_zero = 1;
        for (int i = 0; i < 6; i++) if (src_mac[i]) { all_zero = 0; break; }
        if (all_zero) { src_mac[5] = 0x01; }

        int packet_len = (int)gn_len + 14;
        uint8_t *packet = malloc(packet_len);
        if (!packet) return;

        uint8_t dst_mac[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        memcpy(packet,     dst_mac, 6);
        memcpy(packet + 6, src_mac, 6);
        packet[12] = (GN_ETHER_TYPE >> 8) & 0xFF;
        packet[13] =  GN_ETHER_TYPE       & 0xFF;
        memcpy(packet + 14, gn_buf, gn_len);
        log_debug("[mac] encap ok | len: %uB", packet_len);
        it2s_rsock_tx_packet(rsock, NULL, (unsigned char *)packet, packet_len);

        /* Print raw packet hex */
        char *tx_hex = malloc(packet_len * 2 + 1);
        if (!tx_hex) { free(packet); return; }
        char *buf_ptr = tx_hex;
        for (int i = 0; i < packet_len; i++)
            buf_ptr += sprintf(buf_ptr, "%02x", packet[i]);
        *buf_ptr = '\0';
        log_info("[phy]-> tx | size: %dB data: %s", packet_len, tx_hex);
        free(tx_hex);
        free(packet);
    } 
    
    /* ublox: full 802.11 frame - MAC decap + LLC decap + FCS strip */
    else if (ctx->access_type == ACCESS_UBLOX) {
        rv = it2s_llc_encap(gn_buf, (uint16_t)gn_len, bufA, &lenA, GN_ETHER_TYPE);
        if (rv) { log_error("[llc] encap failed"); return; }
        log_debug("[llc] encap ok | len: %uB", lenA);

        uint8_t src_mac[6] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        uint8_t dst_mac[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        rv = it2s_mac_encap(bufA, (uint16_t)lenA, bufB, &lenB, src_mac, dst_mac);
        if (rv) { log_error("[mac] encap failed"); return; }
        log_debug("[mac] encap ok | len: %uB", lenB);

        it2s_ublox_t *ublox = (it2s_ublox_t *)ctx->obj;
        it2s_ublox_config_t lcf = ublox->config;
        it2s_ublox_tx_packet(ublox, &lcf, bufB, lenB);

        /* Print raw packet hex */
        char *tx_hex = malloc(lenB * 2 + 1);
        if (!tx_hex) return;
        char *buf_ptr = tx_hex;
        for (int i = 0; i < lenB; i++)
            buf_ptr += sprintf(buf_ptr, "%02x", bufB[i]);
        *buf_ptr = '\0';
        log_info("[phy]-> tx | size: %uB data: %s", lenB, tx_hex);
        free(tx_hex);
    }
}

int generate_cam_packet(unsigned char* packet) {
    uint8_t cam_buffer[512];
    int cam_length;

    /* Build a minimal CAM */
    EI2_CAM_t *cam = calloc(1, sizeof(EI2_CAM_t));
    cam->header.protocolVersion = 2;
    cam->header.messageId       = EI2_MessageId_cam;
    cam->header.stationId       = 1;
    cam->cam.generationDeltaTime = 0;
    cam->cam.camParameters.basicContainer.stationType = 5;
    cam->cam.camParameters.basicContainer.referencePosition.latitude = 900000001;
    cam->cam.camParameters.basicContainer.referencePosition.longitude = 1800000001;
    cam->cam.camParameters.basicContainer.referencePosition.positionConfidenceEllipse.semiMajorAxisLength = 4095;
    cam->cam.camParameters.basicContainer.referencePosition.positionConfidenceEllipse.semiMinorAxisLength = 4095;
    cam->cam.camParameters.basicContainer.referencePosition.positionConfidenceEllipse.semiMajorAxisOrientation = 3601;
    cam->cam.camParameters.basicContainer.referencePosition.altitude.altitudeValue = 800001;
    cam->cam.camParameters.basicContainer.referencePosition.altitude.altitudeConfidence = 15;
    cam->cam.camParameters.highFrequencyContainer.present =EI2_HighFrequencyContainer_PR_basicVehicleContainerHighFrequency;
    EI2_BasicVehicleContainerHighFrequency_t *bvc = &cam->cam.camParameters.highFrequencyContainer.choice.basicVehicleContainerHighFrequency;
    bvc->heading.headingValue = 3601;
    bvc->heading.headingConfidence = 127;
    bvc->speed.speedValue = 16383;
    bvc->speed.speedConfidence = 127;
    bvc->driveDirection = 2;
    bvc->vehicleLength.vehicleLengthValue = 1023;
    bvc->vehicleLength.vehicleLengthConfidenceIndication = 4;
    bvc->vehicleWidth = 62;
    bvc->longitudinalAcceleration.value = 161;
    bvc->longitudinalAcceleration.confidence = 102;
    bvc->curvature.curvatureValue = 1023;
    bvc->curvature.curvatureConfidence = 0;
    bvc->curvatureCalculationMode = 2;
    bvc->yawRate.yawRateValue = 32767;
    bvc->yawRate.yawRateConfidence = 8;

    /* UPER encode */
    asn_enc_rval_t enc = uper_encode_to_buffer(&asn_DEF_EI2_CAM, NULL, cam, cam_buffer, sizeof(cam_buffer));
    if (enc.encoded == -1) {
        log_error("[cam] UPER encode failed (%s)", enc.failed_type->name);
        ASN_STRUCT_FREE(asn_DEF_EI2_CAM, cam);
        return -1;
    }
    cam_length = (enc.encoded + 7) / 8;
    ASN_STRUCT_FREE(asn_DEF_EI2_CAM, cam);
    log_debug("[cam] UPER encode ok | len: %dB", cam_length);

    /* GN/BTP encap */
    uint32_t gn_len = 0;
    if (encap_btp_gn(BTP_PORT_CAM, cam_buffer, (uint32_t)cam_length, packet, &gn_len) < 0)
        return -1;
    return (int) gn_len;
}

int generate_denm_packet(unsigned char* packet) {
    uint8_t denm_buffer[512];
    int denm_length;

    /* Build minimal DENM */
    EI2_DENM_t *denm = calloc(1, sizeof(EI2_DENM_t));
    denm->header.protocolVersion = 2;
    denm->header.messageId = EI2_MessageId_denm;
    denm->header.stationId = 1;
    denm->denm.management.actionId.originatingStationId = 1;
    denm->denm.management.actionId.sequenceNumber = 1;
    asn_long2INTEGER(&denm->denm.management.detectionTime, 0);
    asn_long2INTEGER(&denm->denm.management.referenceTime, 0);
    denm->denm.management.eventPosition.latitude = 900000001;
    denm->denm.management.eventPosition.longitude = 1800000001;
    denm->denm.management.eventPosition.positionConfidenceEllipse.semiMajorConfidence = 4095;
    denm->denm.management.eventPosition.positionConfidenceEllipse.semiMinorConfidence = 4095;
    denm->denm.management.eventPosition.positionConfidenceEllipse.semiMajorOrientation = 3601;
    denm->denm.management.eventPosition.altitude.altitudeValue = 800001;
    denm->denm.management.eventPosition.altitude.altitudeConfidence = 15;
    denm->denm.management.termination = NULL;
    denm->denm.management.awarenessDistance = NULL;
    denm->denm.management.awarenessTrafficDirection = NULL;
    denm->denm.management.validityDuration = NULL;
    denm->denm.management.transmissionInterval = NULL;
    denm->denm.management.stationType = 5;
    denm->denm.situation = NULL;
    denm->denm.location  = NULL;
    denm->denm.alacarte  = NULL;

    /* UPER encode */
    asn_enc_rval_t enc = uper_encode_to_buffer(&asn_DEF_EI2_DENM, NULL, denm, denm_buffer, sizeof(denm_buffer));
    if (enc.encoded == -1) {
        log_error("[denm] UPER encode failed (%s)", enc.failed_type->name);
        ASN_STRUCT_FREE(asn_DEF_EI2_DENM, denm);
        return -1;
    }
    denm_length = (enc.encoded + 7) / 8;
    ASN_STRUCT_FREE(asn_DEF_EI2_DENM, denm);
    log_debug("[denm] UPER encode ok | len: %dB", denm_length);

    /* GN/BTP encap */
    uint32_t gn_len = 0;
    if (encap_btp_gn(BTP_PORT_DENM, denm_buffer, (uint32_t)denm_length, packet, &gn_len) < 0)
        return -1;
    return (int) gn_len;
}

void* producer_loop(void* arg) {
    producer_ctx_t* ctx = (producer_ctx_t*) arg;
    uint8_t packet_buffer[IT2S_WIFI_MAX_PACKET_SIZE];
    int gn_len;

    while (1) {
        gn_len = generate_cam_packet(packet_buffer);
        if (gn_len > 0)
            tx_packet(ctx, packet_buffer, (uint32_t)gn_len);

        sleep(1);

        gn_len = generate_denm_packet(packet_buffer);
        if (gn_len > 0)
            tx_packet(ctx, packet_buffer, (uint32_t)gn_len);

        sleep(1);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        log_error("missing arguments... usage: its_producer <rsock|ublox> <interface>");
        return 1;
    }

    char* access_technology = argv[1];
    char* interface = argv[2];
    pthread_t tx_thread;
    producer_ctx_t* ctx = malloc(sizeof(producer_ctx_t));

    if (!strcmp(access_technology, "rsock")) {
        it2s_rsock_t* rsock = it2s_rsock_init((unsigned char*) interface);
        ctx->access_type = ACCESS_RSOCK;
        ctx->obj = rsock;
        pthread_create(&tx_thread, NULL, producer_loop, ctx);
    }

    else if (!strcmp(access_technology, "ublox")) {
        it2s_ublox_t* ublox = it2s_ublox_init(UBLOX_CHANNEL, UBLOX_RADIO_C, UBLOX_CHANNEL_CONFIG, UBLOX_ANTENNA, (char*) UBLOX_MCS, UBLOX_POWER);
        ctx->access_type = ACCESS_UBLOX;
        ctx->obj = ublox;
        pthread_create(&tx_thread, NULL, producer_loop, ctx);
    }

    else {
        log_error("unsupported access technology: %s", access_technology);
        free(ctx);
        return 1;
    }

    pthread_join(tx_thread, NULL);
    free(ctx);
    return 0;
}
