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

#include <unistd.h>
#include "common.h"
#include "hws_cipher_key_check.h"

// Define static constant members
const short int HwsCipherKeyCheck::SUCCESS_FLAG;
const short int HwsCipherKeyCheck::FAIL_FLAG;

int HwsCipherKeyCheck::init(std::string& path){
    passwd_path = path;
    lastuse_path = path + ".lastuse";
    error_path = path + ".error";
    isInit = true;
    // passwd file 与 .lastuse 文件内容一致，不更新.lastuse文件
    if (isFileExist(lastuse_path) && compareFiles(passwd_path, lastuse_path)){
        return SUCCESS_FLAG;
    }
    if (copyFile(passwd_path, lastuse_path) != SUCCESS_FLAG){
        return FAIL_FLAG;
    }
    return SUCCESS_FLAG;
}

bool HwsCipherKeyCheck::isFileExist(const std::string& filePath) {
    struct stat buffer;
    if (stat(filePath.c_str(), &buffer) != 0) {
        return false;
    }
    return true;
}

int HwsCipherKeyCheck::writeFile(std::string filePath, const unsigned char *content, int length, int flags, int mode) {
    int file = open(filePath.c_str(), flags, mode);

    if (file == -1) {
        S3FS_PRN_ERR("open file failed.\n");
        return FAIL_FLAG;
    }

    int len = write(file, content, length);

    if (len != length) {
        S3FS_PRN_ERR("write file failed.\n");
        close(file);
        return FAIL_FLAG;
    }

    if (close(file) < 0) {
        S3FS_PRN_ERR("close file failed.\n");
        return FAIL_FLAG;
    }

    return len;
}

/*
 * 安全读取函数
 * 由于一些异常情况，read可能存在读取字节数和预期读取字节数不一致的情况
 * 若读取到的字节数小于预期读取字节数，断点续读
 * 若读取失败3次，返回错误
 * */
int safeRead(int fd, char *buff, unsigned int size) {
    int retryTimes = 3;
    unsigned int readBytes = 0;
    char *pBuff = buff;
    do {
        int bytes = read(fd, pBuff, size);
        if (bytes > 0) {
            size -= bytes;
            readBytes += bytes;
            pBuff += sizeof(char) * bytes;
        } else if (bytes < 0) {
            if (retryTimes) {
                retryTimes--;
                continue;
            }
            return bytes;
        } else {
            // 文件末尾（EOF）
            break;
        }
    } while (size > 0);
    return readBytes;
}

bool HwsCipherKeyCheck::compareFiles(const std::string path1, const std::string path2){
    // 打开文件
    int file1 = open(path1.c_str(), O_RDONLY);
    int file2 = open(path2.c_str(), O_RDONLY);
    if (file1 == -1 || file2 == -1) {
        S3FS_PRN_ERR("Failed to open file: %s or %s", path1.c_str(), path2.c_str());
        if (file1 != -1) close(file1);
        if (file2 != -1) close(file2);
        return false;
    }

    const unsigned int BUFF_SIZE = 1024;
    char buff1[BUFF_SIZE];
    char buff2[BUFF_SIZE];
    unsigned long long totalBytesRead1 = 0;
    unsigned long long totalBytesRead2 = 0;
    bool filesEqual = true;

    // 读取两个文件直到至少有一个文件结束
    while (true) {
        int bytesRead1 = safeRead(file1, buff1, BUFF_SIZE);
        int bytesRead2 = safeRead(file2, buff2, BUFF_SIZE);

        // 检查读取错误
        if (bytesRead1 < 0 || bytesRead2 < 0) {
            S3FS_PRN_ERR("Error during file read: %d or %d", bytesRead1, bytesRead2);
            filesEqual = false;
            break;
        }

        // 更新总读取字节数
        totalBytesRead1 += bytesRead1;
        totalBytesRead2 += bytesRead2;

        // 检查读取的数据量是否相同
        if (bytesRead1 != bytesRead2) {
            filesEqual = false;
            break;
        }

        if (bytesRead1 == 0 && bytesRead2 == 0) {
            if (totalBytesRead1 != totalBytesRead2) {
                S3FS_PRN_INFO("Files have same content but different sizes: %llu vs %llu bytes",
                              totalBytesRead1, totalBytesRead2);
                filesEqual = false;
            }
            break;
        }

        // 比较读取的内容
        if (memcmp(buff1, buff2, bytesRead1) != 0) {
            filesEqual = false;
            break;
        }
    }

    // 关闭文件
    close(file1);
    close(file2);

    return filesEqual;
}

int HwsCipherKeyCheck::copyFile(std::string srcPath, std::string dstPath, int flags, int mode){
    int buffSize = 1024;
    char buff[buffSize];
    int len = 0;
    int srcFd = open(srcPath.c_str(), O_RDONLY);
    int dstFd = open(dstPath.c_str(), flags, mode);
    if (srcFd == -1 || dstFd == -1) {
        S3FS_PRN_ERR("Failed to open file: %s or %s", srcPath.c_str(), dstPath.c_str());
        if (srcFd != -1) close(srcFd);
        if (dstFd != -1) close(dstFd);
        return FAIL_FLAG;
    }

    while ((len = read(srcFd, buff, buffSize)) > 0) {
        if (write(dstFd, buff, len) != len) {
            S3FS_PRN_ERR("write file failed.\n");
            close(srcFd);
            close(dstFd);
            return FAIL_FLAG;
        }
    }
    if (len < 0) {
        S3FS_PRN_ERR("read file failed.\n");
    }

    close(srcFd);
    close(dstFd);
    return (len < 0) ? FAIL_FLAG : SUCCESS_FLAG;
}

int HwsCipherKeyCheck::updateSuccess() {
    if (!isReady()){
        return SUCCESS_FLAG;
    }
    if (!isFileExist(passwd_path)){
        S3FS_PRN_ERR("passwd file not exist.\n");
        return FAIL_FLAG;
    }
    // passwd file 与 .lastuse 文件内容一致，不更新.lastuse文件
    if (isFileExist(lastuse_path) && compareFiles(passwd_path, lastuse_path)){
        return SUCCESS_FLAG;
    }

    if (copyFile(passwd_path, lastuse_path) != 0){
        return FAIL_FLAG;
    }
    if (isFileExist(error_path)){
        remove(error_path.c_str());
    }
    return SUCCESS_FLAG;
}

int HwsCipherKeyCheck::updateFail(const char* errorMsg) {
    if (!isReady()){
        return SUCCESS_FLAG;
    }
    int contentLen = strlen(errorMsg);
    if (writeFile(error_path, reinterpret_cast<const unsigned char*>(errorMsg), contentLen) != contentLen){
        return FAIL_FLAG;
    }
    return SUCCESS_FLAG;
}