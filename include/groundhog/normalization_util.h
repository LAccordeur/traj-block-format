//
// Created by yangguo on 11/10/22.
//

#ifndef TRAJ_BLOCK_FORMAT_NORMALIZATION_UTIL_H
#define TRAJ_BLOCK_FORMAT_NORMALIZATION_UTIL_H

#define PRECISION (24)

int normalize_longitude(double lon);

int normalize_latitude(double lat);

long generate_zcurve_value(int x, int y, int z);

#endif //TRAJ_BLOCK_FORMAT_NORMALIZATION_UTIL_H
