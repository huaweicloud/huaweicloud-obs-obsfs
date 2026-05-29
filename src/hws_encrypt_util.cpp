#include <stdio.h>
#include <string.h>
#include <string>
#include <stdint.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include <unistd.h>
#include <pwd.h>
#include <iostream>
#include <stdlib.h>
#include <openssl/aes.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <cassert>
#include <limits>
#include <stdexcept>
#include <cctype>

#include <fstream>
#include <list>
#include <iostream>
#include <vector>
#include <algorithm>

#include "common.h"
#include "hws_encrypt_util.h"
#include "string_util.h"
#include <thread>
#include <stdlib.h>
#include "hws_fs_util.h"


#ifdef WIN32
#pragma comment(lib, "libeay32.lib")
#pragma comment(lib, "ssleay32.lib")
#endif

using namespace std;
typedef std::list <std::string> readline_t;
const unsigned char componentDefault[COMPONENT_SIZE] = "obsfs is a file system tool provided by Object Storage"
                                                       " Servicefor mounting OBS parallel file systems to Linux operating systems";
const char b64_table[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const unsigned char reverse_table[128] = {
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
        64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
        64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64
};

// 加密总程序
int HwsEncryption::encryptAkAndSkToPasswdFile(std::string accessKey, std::string &secretKey, std::string securityToken,
                                              std::string passwd_file) {

    //1.各种初始化，准备好rootkey、iv 和 workkey
    // 1.1 生成keyComponent、salt并保存至文件
    if (genAllRootKeyComponents() != SUCCESS_FLAG) {
        S3FS_PRN_ERR("generate root key component failed.\n");
        return FAIL_FLAG;
    }

    // 1.2 生成workKey
    unsigned char workKey[COMPONENT_SIZE] = {0};
    RAND_bytes(workKey, COMPONENT_SIZE);
    std::string workKeyStr = std::string((char *)workKey, COMPONENT_SIZE);


    // 1.3 生成iv
    unsigned char ivValue[AES_BLOCK_SIZE] = {0};
    RAND_bytes(ivValue, AES_BLOCK_SIZE);
    std::string ivStr = std::string((char *)ivValue, AES_BLOCK_SIZE);

    // 1.4 根据1.1生成的加密材料，生成rootkey
    if (getRootKey() != SUCCESS_FLAG) {
        return FAIL_FLAG;
    }

    // 2.对aksktoken做加密
    // 2.1 加密ak
    std::string accessKeyEncrypt = "";
    if (encryptDataByWorkKey(accessKey, accessKeyEncrypt, workKeyStr, ivStr) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("encrypt accessKey failed.\n");
        return FAIL_FLAG;
    } else {
        S3FS_PRN_WARN("accessKeyEncrypt=%s\n", accessKeyEncrypt.c_str());
    }

    // 2.2 加密sk
    std::string secretKeyEncrypt = "";
    if (encryptDataByWorkKey(secretKey, secretKeyEncrypt, workKeyStr, ivStr) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("encrypt secretKey failed.\n");
        return FAIL_FLAG;
    } else {
        S3FS_PRN_WARN("secretKey Encrypt success\n");
    }

    std::string finalEncryptionResult = encrypted_flag + "\n" + accessKeyEncrypt + ":" + secretKeyEncrypt;

    // 2.3 加密token
    if (!securityToken.empty()) {
        std::string securityTokenEncrypt = "";
        if (encryptDataByWorkKey(securityToken, securityTokenEncrypt, workKeyStr, ivStr) != SUCCESS_FLAG) {
            S3FS_PRN_ERR("encrypt security token failed.\n");
            return FAIL_FLAG;
        } else {
            finalEncryptionResult += ":" + securityTokenEncrypt;
            S3FS_PRN_WARN("securityToken Encrypt success\n");
        }
    }

    // 3.要把对应关系存起来
    if (writeRelation(accessKeyEncrypt, workKeyStr, ivStr) != SUCCESS_FLAG) {
        return FAIL_FLAG;
    }

    // 4.将加密后的信息存至用户指定文件中（保存加密后的ak和sk至文件）
    if (writeFile(passwd_file, reinterpret_cast<unsigned char *>((char *) finalEncryptionResult.c_str()),
                  finalEncryptionResult.length()) == FAIL_FLAG) {
        return FAIL_FLAG;
    }

    // 5.退出前清理
    memset(rootKey, 0, sizeof(rootKey));
    memset(workKey, 0, sizeof(workKey));
    workKeyStr = "";
    secretKey = "";
    securityToken = "";
    return SUCCESS_FLAG;
}

int HwsEncryption::writeRelation(std::string &accessKeyEncrypt, std::string &workKeyStr, std::string &ivStr) {

    std::string rootKeyStr = std::string((char *)rootKey, COMPONENT_SIZE);

    //用rootkey对workkey做一下加密
    std::string relationEncrypt = "";
    if(encryptDataByWorkKey(workKeyStr, relationEncrypt, rootKeyStr, ivStr)!= SUCCESS_FLAG) {
        S3FS_PRN_ERR("encrypt workKeyStr failed.\n");
        return FAIL_FLAG;
    }

    //拼接字符串 accessKeyEncrypt密文（然后base64） + workKey密文（用rootKey加密的） + base64编码后的iv
    std::string relationStr = accessKeyEncrypt + ":" + relationEncrypt + ":" + Base64Encode(ivStr) + "\n";
    if (appendFile(relationFilePath, reinterpret_cast<unsigned char *>((char *) relationStr.c_str()),
                   relationStr.length()) == FAIL_FLAG) {
        return FAIL_FLAG;
    }
    return SUCCESS_FLAG;
}

void HwsEncryption::log(const char *message) {
    char log_buf[1024] = {0};
    time_t time_now;
    time(&time_now);
    struct tm cur_time;
    localtime_r(&time_now, &cur_time);
    snprintf(log_buf, sizeof(log_buf),
             "[%04d-%02d-%02d %02d:%02d:%02d] %s by %s\n",
             cur_time.tm_year + 1900, cur_time.tm_mon + 1,
             cur_time.tm_mday, cur_time.tm_hour, cur_time.tm_min,
             cur_time.tm_sec, message, getenv("USER"));

    mode_t old_umask = umask(0);
    writeFile(logPath, (const unsigned char*)log_buf, strlen(log_buf), O_WRONLY | O_CREAT | O_APPEND, 0622);
    umask(old_umask);
}

// 一. 生成keyComponent、salt并保存至文件。
int HwsEncryption::genAllRootKeyComponents() {

    // 1. 生成并保存keyComponent
    if (!isFileExist(keyComponentFilePath)) {
        unsigned char keyComponent[COMPONENT_SIZE] = {0};
        RAND_bytes(keyComponent, COMPONENT_SIZE);
        if (writeFile(keyComponentFilePath, keyComponent, COMPONENT_SIZE) != COMPONENT_SIZE) {
            return FAIL_FLAG;
        }
        log("keyComponent generated");
    }


    // 2. 生成并保存salt
    if (!isFileExist(saltFilePath)) {
        unsigned char saltValue[COMPONENT_SIZE] = {0};
        RAND_bytes(saltValue, COMPONENT_SIZE);
        if (writeFile(saltFilePath, saltValue, COMPONENT_SIZE) != COMPONENT_SIZE) {
            return FAIL_FLAG;
        }
        log("saltValue generated");
    }

    return SUCCESS_FLAG;
}

bool HwsEncryption::isFileExist(std::string filePath) {
    struct stat buffer;
    if (stat(filePath.c_str(), &buffer) != 0) {
        return false;
    }
    if (buffer.st_size <= 0) {
        return false;
    }
    return true;
}

int HwsEncryption::writeFile(std::string filePath, const unsigned char *content, int length, int flags, int mode) {
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

int HwsEncryption::appendFile(std::string filePath, const unsigned char *content, int length) {
    char resolved_path[PATH_MAX];
    if(!verifyPath(filePath.c_str(), resolved_path, false)) {
        return FAIL_FLAG;
    }

    int file = open(resolved_path, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR);

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

int HwsEncryption::readFile(std::string filePath, unsigned char *buffer, int length) {
    char resolved_path[PATH_MAX];
    if(!verifyPath(filePath.c_str(), resolved_path, false)) {
        return FAIL_FLAG;
    }

    int file = open(resolved_path, O_RDWR, S_IRUSR | S_IWUSR);
    if (file == -1) {
        S3FS_PRN_ERR("open file failed.\n");
        return FAIL_FLAG;
    }

    int len = read(file, buffer, length);
    if (len != length) {
        S3FS_PRN_ERR("read file failed.\n");
        close(file);
        return FAIL_FLAG;
    }

    if (close(file) < 0) {
        S3FS_PRN_ERR("close file failed.\n");
        return FAIL_FLAG;
    }

    return len;
}

// 二. 用workKey加密数据（plainData → cipherData）
int HwsEncryption::encryptDataByWorkKey(std::string &plainData, std::string &cipherData, const std::string &workKeyStr,
                                        const std::string &ivStr) {

    unsigned char workKey[COMPONENT_SIZE];
    unsigned char iv[AES_BLOCK_SIZE];
    memcpy(workKey, workKeyStr.c_str(), workKeyStr.length());
    memcpy(iv, ivStr.c_str(), ivStr.length());

    // 1. AES加密
    std::string aesCipherData = "";
    if (encryptDataByAES256CBC(plainData, aesCipherData, workKey, iv) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("use root key encrypt ak sk failed.\n");
        return FAIL_FLAG;
    }

    // 2. Base64加密
    cipherData = Base64Encode(aesCipherData);
    if (cipherData.empty()) {
        S3FS_PRN_ERR("Base64 failed.\n");
        return FAIL_FLAG;
    }

    return SUCCESS_FLAG;

}

int HwsEncryption::decryptDataByWorkKey(std::string &plainData, std::string &cipherData, const std::string &workKeyStr, const std::string &ivStr) {
    unsigned char workKey[COMPONENT_SIZE];
    unsigned char iv[AES_BLOCK_SIZE];
    memcpy(workKey, workKeyStr.c_str(), workKeyStr.length());
    memcpy(iv, ivStr.c_str(), ivStr.length());

    // 1. Base64解密
    std::string plainDataDecode = Base64Decode(plainData);
    if (plainDataDecode.empty()) {
        S3FS_PRN_ERR("Base64decode failed.\n");
        return FAIL_FLAG;
    }

    // 2. AES解密
    if (decryptDataByAES256CBC(plainDataDecode, cipherData, workKey, iv) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("use root key decrypt ak sk failed.\n");
        return FAIL_FLAG;
    }
    return SUCCESS_FLAG;
}

// 解密数据（cipherData → plainData）
int HwsEncryption::genRootKeyAndDecryptData(std::string &key, std::string &cipherData, std::string &plainData) {
    // 1. 根据加密材料计算rootKey
    if (getRootKey() != SUCCESS_FLAG) {
        S3FS_PRN_ERR("generate root key by PBKDF2 failed.\n");
        return FAIL_FLAG;
    }

    // 2.按key获取对应解密的workkey 和iv
    std::string workKey = "";
    std::string ivFromRela = "";
    if (getEncryptInfoFromRelationFile(key, workKey, ivFromRela) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("workkey and iv get fail.\n");
        return FAIL_FLAG;
    }

    std::string realIv = Base64Decode(ivFromRela);

    // 3.用rootkey 把workkey解密下，准备好明文的workkey
    std::string workkeyDecrypt = "";
    std::string rootKeyStr = std::string((char *)rootKey, COMPONENT_SIZE);
    if (decryptDataByWorkKey(workKey, workkeyDecrypt, rootKeyStr, realIv) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("use root key decrypt workKeyfailed.\n");
        return FAIL_FLAG;
    }

    // 4. 将待解密字段用明文workkey 做AES解密
    if (decryptDataByWorkKey(cipherData, plainData, workkeyDecrypt, realIv) != SUCCESS_FLAG) {
        S3FS_PRN_ERR("use root key decrypt ak sk failed.\n");
        return FAIL_FLAG;
    }

    //本轮结束前清理信息
    workkeyDecrypt = "";
    memset(rootKey, 0, sizeof(rootKey));
    return SUCCESS_FLAG;
}

// 加密数据的子方法一：计算rootKey
int HwsEncryption::getRootKey() {

    // 1. 读keyComponentFilePath
    unsigned char keyComponent[COMPONENT_SIZE] = {0};
    if (readFile(keyComponentFilePath, keyComponent, COMPONENT_SIZE) != COMPONENT_SIZE) {
        S3FS_PRN_ERR("read keyComponent file failed.\n");
        return FAIL_FLAG;
    }

    // 2. 计算temComponent
    unsigned char tmpComponent[COMPONENT_SIZE] = {0};
    for (int i = 0; i < COMPONENT_SIZE; i++) {
        tmpComponent[i] = keyComponent[i] ^ componentDefault[i];
    }

    // 3. 读saltFilePath
    unsigned char saltValue[COMPONENT_SIZE] = {0};
    if (readFile(saltFilePath, saltValue, COMPONENT_SIZE) != COMPONENT_SIZE) {
        S3FS_PRN_ERR("read salt file failed.\n");
        return FAIL_FLAG;
    }

    // 4. 计算rootKey
    if (PKCS5_PBKDF2_HMAC((const char *) tmpComponent,
                          COMPONENT_SIZE,
                          (const unsigned char *) saltValue,
                          COMPONENT_SIZE,
                          ITERATION_NUM,
                          EVP_sha256(),
                          32,
                          rootKey) == 0) {
        S3FS_PRN_ERR("generate root key by PBKDF2 failed.\n");
        return FAIL_FLAG;
    }

    return SUCCESS_FLAG;
}

// 加密数据的子方法二：做AES加密
int HwsEncryption::encryptDataByAES256CBC(std::string &strBeforeEncrypt, std::string &strAfterEncrypt,
                                          unsigned char *workKey, unsigned char *iv) {
    // 1. 得AES-KEY
    AES_KEY aes_key;
    if (AES_set_encrypt_key(workKey, 256, &aes_key) < 0) {
        return FAIL_FLAG;
    }

    // 2. 处理ak/sk/token
    std::string data_bak = strBeforeEncrypt;
    unsigned int data_length = data_bak.length();

    unsigned int padding = 0;
    if (data_bak.length() % (AES_BLOCK_SIZE) > 0) {
        padding = AES_BLOCK_SIZE - data_bak.length() % (AES_BLOCK_SIZE);
    }
    data_length += padding;
    while (padding > 0) {
        data_bak += '\0';
        padding--;
    }

    // 3. 利用前2步的数据，做AES加密
    for (unsigned int i = 0; i < data_length / (AES_BLOCK_SIZE); i++) {
        std::string str16 = data_bak.substr(i * AES_BLOCK_SIZE, AES_BLOCK_SIZE);
        unsigned char out[AES_BLOCK_SIZE];
        ::memset(out, 0, AES_BLOCK_SIZE);
        AES_cbc_encrypt((const unsigned char *) str16.c_str(), out, AES_BLOCK_SIZE, &aes_key, iv, AES_ENCRYPT);
        strAfterEncrypt += std::string((const char *) out, AES_BLOCK_SIZE);
    }
    return SUCCESS_FLAG;
}

// 解密数据的子方法二：AES解密
int HwsEncryption::decryptDataByAES256CBC(std::string &strBeforeDecrypt, std::string &strAfterDecrypt,
                                          unsigned char *workKey, unsigned char *iv) {

    AES_KEY aes_key;
    if (AES_set_decrypt_key(workKey, 256, &aes_key) < 0) {
        return FAIL_FLAG;
    }
    std::string tmpStrAfterDecrypt = "";
    for (unsigned int i = 0; i < strBeforeDecrypt.length() / AES_BLOCK_SIZE; i++) {
        std::string str16 = strBeforeDecrypt.substr(i * AES_BLOCK_SIZE, AES_BLOCK_SIZE);
        unsigned char out[AES_BLOCK_SIZE];
        ::memset(out, 0, AES_BLOCK_SIZE);
        AES_cbc_encrypt((const unsigned char *) str16.c_str(), out, AES_BLOCK_SIZE, &aes_key, iv, AES_DECRYPT);
        tmpStrAfterDecrypt += std::string((const char *) out, AES_BLOCK_SIZE);
    }
    strAfterDecrypt = tmpStrAfterDecrypt;

    return SUCCESS_FLAG;
}

// 加密数据的子方法三：Base64加密
std::string HwsEncryption::Base64Encode(const std::string &binData)
{
    using std::string;
    using std::numeric_limits;

    if (binData.size() > (numeric_limits<string::size_type>::max() / 4u) * 3u) {
        S3FS_PRN_ERR("Converting too large a string to base64.");
        return NULL;
    }

    const std::size_t binLen = binData.size();
    // Use = signs so the end is properly padded.
    string retVal((((binLen + 2) / 3) * 4), '=');
    std::size_t outPos = 0;
    int bits_collected = 0;
    unsigned int accumulator = 0;
    const string::const_iterator binEnd = binData.end();

    for (string::const_iterator i = binData.begin(); i != binEnd; ++i) {
        accumulator = (accumulator << 8) | (static_cast<unsigned char>(*i) & 0xffu);
        bits_collected += 8;
        while (bits_collected >= 6) {
            bits_collected -= 6;
            retVal[outPos++] = b64_table[(accumulator >> bits_collected) & 0x3fu];
        }
    }
    if (bits_collected > 0) { // Any trailing bits that are missing.
        assert(bits_collected < 6);
        accumulator <<= 6 - bits_collected;
        retVal[outPos++] = b64_table[accumulator & 0x3fu];
    }
    assert(outPos >= (retVal.size() - 2));
    assert(outPos <= retVal.size());
    return retVal;
}

// 解密数据的子方法一：Base64解密
std::string HwsEncryption::Base64Decode(const std::string &ascData)
{
    using std::string;
    string retVal;
    const string::const_iterator last = ascData.end();
    int bits_collected = 0;
    unsigned int accumulator = 0;
    for (string::const_iterator i = ascData.begin(); i != last; ++i) {
        const int c = *i;
        if (std::isspace(c) || c == '=') {
            // Skip whitespace and padding. Be liberal in what you accept.
            continue;
        }
        if ((c > 127) || (c < 0) || (reverse_table[c] > 63)) {
            S3FS_PRN_ERR("This contains characters not legal in a base64 encoded string.");
            return NULL;
        }
        accumulator = (accumulator << 6) | reverse_table[c];
        bits_collected += 6;
        if (bits_collected >= 8) {
            bits_collected -= 8;
            retVal += static_cast<char>((accumulator >> bits_collected) & 0xffu);
        }
    }
    return retVal;
}

std::string HwsEncryption::getUserName() {
    return getpwuid(getuid())->pw_name;
}


int HwsEncryption::getEncryptInfoFromRelationFile(const std::string &akEncrypt, std::string &workKey, std::string &iv) {
    std::string line;
    size_t first_pos;
    size_t last_pos;
    readline_t linelist;
    readline_t::iterator iter;

    char resolved_path[PATH_MAX];
    if(!verifyPath(relationFilePath.c_str(), resolved_path, false)) {
        return -1;
    }

    ifstream PF(resolved_path);
    if (!PF.good()) {
        S3FS_PRN_EXIT("could not open relation file : %s", relationFilePath.c_str());
        return -1;
    }

    // read each line
    while (getline(PF, line)) {
        line = trim(line);
        if (0 == line.size()) {
            continue;
        }
        linelist.push_back(line);
    }
    for (iter = linelist.begin(); iter != linelist.end(); ++iter) {
        int count_colon = std::count(iter->begin(), iter->end(), ':');
        if (count_colon != 2) {
            S3FS_PRN_EXIT("relation file formate err: %s", relationFilePath.c_str());
            return -1;
        }
        first_pos = iter->find_first_of(":");
        last_pos = iter->find_last_of(":");
        string akEncryptInRealtion = trim(iter->substr(0, first_pos));
        string workKeyEncrypt = trim(iter->substr(first_pos + 1, last_pos - first_pos - 1));
        string ivInRealtion = trim(iter->substr(last_pos + 1, string::npos));

        if (akEncryptInRealtion.compare(akEncrypt) == 0) {
            workKey = workKeyEncrypt;
            iv = ivInRealtion;
            return SUCCESS_FLAG;
        }
    }

    return FAIL_FLAG;
}

bool HwsEncryption::isEndWith(std::string fullStr, std::string substr) {
    return fullStr.rfind(substr) == (fullStr.length() - substr.length());
}

std::string HwsEncryption::getKeyFilePath(std::string keyFileName) {
    if (getUserName().compare(rootUserPath) == 0) {
        return "/" + rootUserPath + "/" + keyFileName;
    } else {
        std::string homePathStr = getenv("HOME");
        if (isEndWith(homePathStr,"/")) {
            return homePathStr + keyFileName;
        } else {
            return homePathStr + "/" + keyFileName;
        }
    }
}

HwsEncryption::HwsEncryption() : rootKey{0} {
    keyComponentFilePath = getKeyFilePath(keyComponentFilename);
    saltFilePath = getKeyFilePath(saltFilename);
    relationFilePath = getKeyFilePath(relationFilename);
}

HwsEncryption &HwsEncryption::get_instance() {
    static HwsEncryption instance;
    return instance;
}

HwsEncryption::~HwsEncryption() {}