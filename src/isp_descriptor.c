//
// Created by yangguo on 11/28/22.
//

#include <string.h>
#include <malloc.h>
#include "isp_descriptor.h"
#include "common_util.h"

static const int ISP_TYPE_SIZE = size_of_attribute(struct isp_descriptor, isp_type);
static const int OID_SIZE = size_of_attribute(struct isp_descriptor, oid);
static const int TIME_MIN_SIZE = size_of_attribute(struct isp_descriptor, time_min);
static const int TIME_MAX_SIZE = size_of_attribute(struct isp_descriptor, time_max);
static const int LON_MIN_SIZE = size_of_attribute(struct isp_descriptor, lon_min);
static const int LON_MAX_SIZE = size_of_attribute(struct isp_descriptor, lon_max);
static const int LAT_MIN_SIZE = size_of_attribute(struct isp_descriptor, lat_min);
static const int LAT_MAX_SIZE = size_of_attribute(struct isp_descriptor, lat_max);
static const int DST_BLOCK_ADDRESS_SIZE = size_of_attribute(struct isp_descriptor, dst_block_address);
static const int DST_BLOCK_SIZE_SIZE = size_of_attribute(struct isp_descriptor, dst_block_size);
static const int SRC_BLOCK_ADDRESSES_COUNT_SIZE = size_of_attribute(struct isp_descriptor, src_block_addresses_count);

static const int ISP_TYPE_OFFSET = 0;
static const int OID_OFFSET = ISP_TYPE_OFFSET + ISP_TYPE_SIZE;
static const int TIME_MIN_OFFSET = OID_OFFSET + OID_SIZE;
static const int TIME_MAX_OFFSET = TIME_MIN_OFFSET + TIME_MIN_SIZE;
static const int LON_MIN_OFFSET = TIME_MAX_OFFSET + TIME_MAX_SIZE;
static const int LON_MAX_OFFSET = LON_MIN_OFFSET + LON_MIN_SIZE;
static const int LAT_MIN_OFFSET = LON_MAX_OFFSET + LON_MAX_SIZE;
static const int LAT_MAX_OFFSET = LAT_MIN_OFFSET + LAT_MIN_SIZE;
static const int DST_BLOCK_ADDRESS_OFFSET = LAT_MAX_OFFSET + LAT_MAX_SIZE;
static const int DST_BLOCK_SIZE_OFFSET = DST_BLOCK_ADDRESS_OFFSET + DST_BLOCK_ADDRESS_SIZE;
static const int SRC_BLOCK_ADDRESSES_COUNT_OFFSET = DST_BLOCK_SIZE_OFFSET + DST_BLOCK_SIZE_SIZE;
static const int SRC_BLOCK_ADDRESSES_OFFSET = SRC_BLOCK_ADDRESSES_COUNT_OFFSET + SRC_BLOCK_ADDRESSES_COUNT_SIZE;

static int calculate_src_blk_addresses_space(int src_block_addresses_count) {
    return src_block_addresses_count * sizeof(int);
}

void serialize_isp_descriptor(struct isp_descriptor *source, void *destination) {
    char *d = destination;
    memcpy(d + ISP_TYPE_OFFSET, &(source->isp_type), ISP_TYPE_SIZE);
    memcpy(d + OID_OFFSET, &(source->oid), OID_SIZE);
    memcpy(d + TIME_MIN_OFFSET, &(source->time_min), TIME_MIN_SIZE);
    memcpy(d + TIME_MAX_OFFSET, &(source->time_max), TIME_MAX_SIZE);
    memcpy(d + LON_MIN_OFFSET, &(source->lon_min), LON_MIN_SIZE);
    memcpy(d + LON_MAX_OFFSET, &(source->lon_max), LON_MAX_SIZE);
    memcpy(d + LAT_MIN_OFFSET, &(source->lat_min), LAT_MIN_SIZE);
    memcpy(d + LAT_MAX_OFFSET, &(source->lat_max), LAT_MAX_SIZE);
    memcpy(d + DST_BLOCK_ADDRESS_OFFSET, &(source->dst_block_address), DST_BLOCK_ADDRESS_SIZE);
    memcpy(d + DST_BLOCK_SIZE_OFFSET, &(source->dst_block_size), DST_BLOCK_SIZE_SIZE);
    memcpy(d + SRC_BLOCK_ADDRESSES_COUNT_OFFSET, &(source->src_block_addresses_count), SRC_BLOCK_ADDRESSES_COUNT_SIZE);
    int src_block_addresses_space = calculate_src_blk_addresses_space(source->src_block_addresses_count);
    memcpy(d + SRC_BLOCK_ADDRESSES_OFFSET, source->src_block_addresses, src_block_addresses_space);
}

void deserialize_isp_descriptor(void *source, struct isp_descriptor *destination) {
    char *s = source;
    memcpy(&(destination->isp_type), s + ISP_TYPE_OFFSET, ISP_TYPE_SIZE);
    memcpy(&(destination->oid), s + OID_OFFSET, OID_SIZE);
    memcpy(&(destination->time_min), s + TIME_MIN_OFFSET, TIME_MIN_SIZE);
    memcpy(&(destination->time_max), s + TIME_MAX_OFFSET, TIME_MAX_SIZE);
    memcpy(&(destination->lon_min), s + LON_MIN_OFFSET, LON_MIN_SIZE);
    memcpy(&(destination->lon_max), s + LON_MAX_OFFSET, LON_MAX_SIZE);
    memcpy(&(destination->lat_min), s + LAT_MIN_OFFSET, LAT_MIN_SIZE);
    memcpy(&(destination->lat_max), s + LAT_MAX_OFFSET, LAT_MAX_SIZE);
    memcpy(&(destination->dst_block_address), s + DST_BLOCK_ADDRESS_OFFSET, DST_BLOCK_ADDRESS_SIZE);
    memcpy(&(destination->dst_block_size), s + DST_BLOCK_SIZE_OFFSET, DST_BLOCK_SIZE_SIZE);
    memcpy(&(destination->src_block_addresses_count), s + SRC_BLOCK_ADDRESSES_COUNT_OFFSET, SRC_BLOCK_ADDRESSES_COUNT_SIZE);
    int src_block_addresses_space = calculate_src_blk_addresses_space(destination->src_block_addresses_count);
    int *src_block_addresses = malloc(src_block_addresses_space);
    memcpy(src_block_addresses, s + SRC_BLOCK_ADDRESSES_OFFSET, src_block_addresses_space);
    destination->src_block_addresses = src_block_addresses;
}

void free_isp_descriptor(struct isp_descriptor *descriptor) {
    if (descriptor->src_block_addresses != NULL) {
        free(descriptor->src_block_addresses);
        descriptor->src_block_addresses = NULL;
    }
}

int calculate_isp_descriptor_space(struct isp_descriptor *descriptor) {
    int size = ISP_TYPE_SIZE + OID_SIZE + TIME_MIN_SIZE + TIME_MAX_SIZE + LON_MIN_SIZE + LON_MAX_SIZE + LAT_MIN_SIZE + LAT_MAX_SIZE + DST_BLOCK_ADDRESS_SIZE + DST_BLOCK_SIZE_SIZE + SRC_BLOCK_ADDRESSES_COUNT_SIZE +
            calculate_src_blk_addresses_space(descriptor->src_block_addresses_count);
}

