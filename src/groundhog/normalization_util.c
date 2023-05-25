//
// Created by yangguo on 11/10/22.
//

#include "groundhog/normalization_util.h"

static const int bins = 1 << PRECISION;
static const double lon_normalizer = bins / (180.0 - (-180));
static const double lat_normalizer = bins / (90.0 - (-90));
static const int max_index = (bins - 1);

int normalize_longitude(double lon) {
    if (lon >= 180) {
        return max_index;
    } else if (lon <= -180) {
        return 0;
    } else {
        return (int) ((lon - (-180)) * lon_normalizer);
    }
}

int normalize_latitude(double lat) {
    if (lat >= 90) {
        return max_index;
    } else if (lat <= -90) {
        return 0;
    } else {
        return (int)((lat - (-90)) * lat_normalizer);
    }
}

static long MaxMask = 0x1fffffL;
static long split_z3(long value) {
    long x = value & MaxMask;
    x = (x | x << 32) & 0x1f00000000ffffL;
    //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
    x = (x | x << 16) & 0x1f0000ff0000ffL;
    //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
    x = (x | x << 8)  & 0x100f00f00f00f00fL;
    //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
    x = (x | x << 4)  & 0x10c30c30c30c30c3L;
    //System.out.println(CurveUtil.bytesToBit(CurveUtil.toBytes(x)));
    return (x | x << 2)      & 0x1249249249249249L;
}

long generate_zcurve_value(int x, int y, int z) {
    long value = split_z3(z) | split_z3(y) << 1 | split_z3(x) << 2;
    return value;
}