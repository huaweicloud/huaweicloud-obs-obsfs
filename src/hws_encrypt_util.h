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

#include <stdio.h>
#include <string.h>
#include <string>

#define AES_BLOCK_SIZE       16
#define COMPONENT_SIZE       128
#define ITERATION_NUM        10000

class HwsEncryption {
private:
    unsigned char rootKey[COMPONENT_SIZE];
    const std::string keyComponentFilename = ".okc";
    const std::string saltFilename = ".oks";
    const std::string relationFilename = ".okr";
    std::string keyComponentFilePath;
    std::string saltFilePath;
    std::string relationFilePath;

    const std::string rootUserPath = "root";
    const std::string nonRootUserPath = "home";
    const short int SUCCESS_FLAG = 0;
    const short int FAIL_FLAG = -1;
    const std::string logPath = "/var/log/obsfs/.keylog";

    HwsEncryption();

    int genAllRootKeyComponents();

    bool isFileExist(std::string filePath);
    int writeFile(std::string filePath, const unsigned char *content, int length,
                  int flags = O_RDWR | O_CREAT | O_TRUNC,
                  int mode = S_IRUSR | S_IWUSR);
    int appendFile(std::string filePath, const unsigned char *content, int length);

    int readFile(std::string filePath, unsigned char *buffer, int length);

    int encryptDataByWorkKey(std::string &plainData, std::string &cipherData, const std::string &workKeyStr, const std::string &ivStr);
    int decryptDataByWorkKey(std::string &plainData, std::string &cipherData, const std::string &workKeyStr, const std::string &ivStr);
    int getRootKey();

    int encryptDataByAES256CBC(std::string &strBeforeEncrypt, std::string &strAfterEncrypt, unsigned char *workKey, unsigned char *iv);

    int decryptDataByAES256CBC(std::string &strBeforeDecrypt, std::string &strAfterDecrypt, unsigned char *workKey, unsigned char *iv);
    std::string Base64Encode(const std::string &binData);

    std::string Base64Decode(const std::string &ascData);

    std::string getUserName();

    bool isEndWith(std::string fullStr, std::string substr);

    std::string getKeyFilePath(std::string keyFileName);

    int getEncryptInfoFromRelationFile(const std::string &akEncrypt, std::string &workKey, std::string &iv);

    int writeRelation(std::string &accessKeyEncrypt, std::string &workKeyStr, std::string &ivStr);

    void log(const char *message);

public:
    const std::string encrypted_flag = "*encrypted_ak_sk*";

    static HwsEncryption &get_instance();

    int encryptAkAndSkToPasswdFile(std::string accessKey, std::string &secretKey, std::string securityToken,
                                   std::string passwd_file);

    int genRootKeyAndDecryptData(std::string &key, std::string &cipherData, std::string &plainData);

    ~HwsEncryption();

    HwsEncryption(const HwsEncryption &) = delete;
    HwsEncryption &operator=(const HwsEncryption &) = delete;
};