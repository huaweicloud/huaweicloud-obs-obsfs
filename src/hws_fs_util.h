/*
 * Copyright (C) 2018. Huawei Technologies Co., Ltd.
 *
 * This program is free software; you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License version 2 and
 * only version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

#ifndef SFSCLIENT_HWS_FS_UTIL_H
#define SFSCLIENT_HWS_FS_UTIL_H

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"

bool verifyPath(const char *path, char *resolvedPath, bool shouldExist);

#endif //SFSCLIENT_HWS_FS_UTIL_H
