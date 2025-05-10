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

#include <string>
#include <fcntl.h>
#include <sys/stat.h>

class HwsCipherKeyCheck {
private:
    std::string passwd_path;
    std::string lastuse_path;
    std::string error_path;
    bool isInit = false;

    HwsCipherKeyCheck(){}

    bool isFileExist(const std::string& filePath);

    int writeFile(std::string filePath, const unsigned char *content, int length,
                  int flags = O_RDWR | O_CREAT | O_TRUNC,
                  int mode = S_IRUSR | S_IWUSR);

    bool compareFiles(const std::string path1, const std::string path2);

    int copyFile(std::string srcPath, std::string dstPath,
                 int flags = O_RDWR | O_CREAT | O_TRUNC,
                 int mode = S_IRUSR | S_IWUSR);

public:

    const static short int SUCCESS_FLAG = 0;
    const static short int FAIL_FLAG = -1;

    ~HwsCipherKeyCheck(){}

    static HwsCipherKeyCheck& GetInstance(){
        static HwsCipherKeyCheck instance;
        return instance;
    }

    int init(std::string& path);

    bool isReady(){
        return isInit;
    }

    //密钥更新成功，删除.error文件，更新.lastuse文件
    int updateSuccess();

    //#密钥更新失败，创建.error文件并写入错误原因
    int updateFail(const char* errorMsg);
};

