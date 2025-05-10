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
    bool res = true;
    // 检查文件是否存在，并获取文件大小
    struct stat stat1;
    struct stat stat2;
    if (stat(path1.c_str(), &stat1) != 0
        || stat(path2.c_str(), &stat2) != 0) {
        S3FS_PRN_ERR("Faild to stat file: %s or %s", path1.c_str(), path2.c_str());
        return false;
    }
    if (stat1.st_size != stat2.st_size){
        S3FS_PRN_INFO("Inconsistent file sizes, file1 size:%ld, file2 size:%ld", stat1.st_size, stat2.st_size);
        return false;
    }

    // 打开文件
    int file1 = open(path1.c_str(), O_RDONLY);
    int file2 = open(path2.c_str(), O_RDONLY);
    if (file1 == -1 || file2 == -1) {
        S3FS_PRN_ERR("Failed to open file: %s or %s", path1.c_str(), path2.c_str());
        if (file1 != -1) close(file1);
        if (file2 != -1) close(file2);
        return false;
    }

    unsigned long long remainSize = stat1.st_size;
    unsigned int readBytes = 0;

    // 设置缓冲区
    const unsigned int BUFF_SIZE = 1024;
    char buff1[BUFF_SIZE];
    char buff2[BUFF_SIZE];

    // 读取文件并比较
    int bytes1, bytes2;
    do {
        readBytes = remainSize >= BUFF_SIZE ? BUFF_SIZE : remainSize;
        bytes1 = safeRead(file1, buff1, readBytes);
        bytes2 = safeRead(file2, buff2, readBytes);

        if (bytes1 < 0 || bytes2 < 0) {
            S3FS_PRN_ERR("Error during file read.");
            res = false;
            break;
        }

        // 读取错误处理
        if (bytes1 != static_cast<int>(readBytes) || bytes2 != static_cast<int>(readBytes)) {
            S3FS_PRN_INFO("Inconsistent read sizes, readBytes:%u, bytes1:%d, bytes:%d.", readBytes, bytes1, bytes2);
            res = false;
            break;
        }

        // 内容不一致
        if (memcmp(buff1, buff2, bytes1) != 0) {
            S3FS_PRN_INFO("The file content is inconsistent.");
            res = false;
            break;
        }

        remainSize -= readBytes;
    } while (remainSize > 0);

    // 关闭文件
    close(file1);
    close(file2);

    return res;
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