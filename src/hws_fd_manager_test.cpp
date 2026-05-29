/*
 * obsfs - FUSE filesystem for Huawei Object Storage Service (OBS)
 *
 * Copyright (c) 2025 Huawei Cloud Computing
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include "gtest.h"
#include "hws_fd_cache.h"
#include "common.h"
#include <memory>
#include <set>

// Test class for HwsFdManager tests
class HwsFdManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clear any existing entities before each test
        auto& manager = HwsFdManager::GetInstance();
        auto entities = manager.GetAllEntities();
        for (const auto& entity : entities) {
            manager.Close(entity->GetInodeNo());
        }
    }

    void TearDown() override {
        // Clean up after each test
        auto& manager = HwsFdManager::GetInstance();
        auto entities = manager.GetAllEntities();
        for (const auto& entity : entities) {
            manager.Close(entity->GetInodeNo());
        }
    }
};

// Test GetAllEntities with no open files
TEST_F(HwsFdManagerTest, GetAllEntities_Empty) {
    auto& manager = HwsFdManager::GetInstance();
    auto entities = manager.GetAllEntities();

    EXPECT_EQ(entities.size(), 0);
    EXPECT_TRUE(entities.empty());
}

// Test GetAllEntities with single file
TEST_F(HwsFdManagerTest, GetAllEntities_SingleFile) {
    auto& manager = HwsFdManager::GetInstance();

    // Open a file
    std::string path = "/test/file.txt";
    uint64_t inode = 12345;
    auto entity = manager.Open(path.c_str(), inode, 0);

    ASSERT_NE(entity, nullptr) << "Failed to open file";

    // Get all entities
    auto entities = manager.GetAllEntities();
    EXPECT_EQ(entities.size(), 1);

    // Verify path matches
    EXPECT_EQ(entities[0]->GetPath(), path);
    EXPECT_EQ(entities[0]->GetInodeNo(), (long long)inode);

    // Clean up
    manager.Close(inode);
}

// Test GetAllEntities with multiple files
TEST_F(HwsFdManagerTest, GetAllEntities_MultipleFiles) {
    auto& manager = HwsFdManager::GetInstance();

    std::vector<std::string> paths = {
        "/test/file1.txt",
        "/test/file2.txt",
        "/test/file3.txt"
    };

    std::vector<uint64_t> inodes = {1001, 1002, 1003};

    // Open multiple files
    for(size_t i = 0; i < paths.size(); i++){
        auto entity = manager.Open(paths[i].c_str(), inodes[i], 0);
        ASSERT_NE(entity, nullptr) << "Failed to open file: " << paths[i];
    }

    // Get all entities
    auto entities = manager.GetAllEntities();
    EXPECT_EQ(entities.size(), 3);

    // Verify all paths present
    std::set<std::string> found_paths;
    for(const auto& entity : entities){
        found_paths.insert(entity->GetPath());
    }

    for(const auto& path : paths){
        EXPECT_NE(found_paths.find(path), found_paths.end())
            << "Path not found: " << path;
    }

    // Clean up
    for(auto inode : inodes){
        manager.Close(inode);
    }
}

// Test GetAllEntitiesUnderPath with no matching files
TEST_F(HwsFdManagerTest, GetAllEntitiesUnderPath_NoMatch) {
    auto& manager = HwsFdManager::GetInstance();

    // Open files in /other directory
    auto entity1 = manager.Open("/other/file1.txt", 2001, 0);
    auto entity2 = manager.Open("/other/file2.txt", 2002, 0);

    ASSERT_NE(entity1, nullptr);
    ASSERT_NE(entity2, nullptr);

    // Get entities under /test (should be empty)
    auto entities = manager.GetAllEntitiesUnderPath("/test");
    EXPECT_EQ(entities.size(), 0);

    // But GetAllEntities should return 2
    auto all_entities = manager.GetAllEntities();
    EXPECT_EQ(all_entities.size(), 2);

    // Clean up
    manager.Close(2001);
    manager.Close(2002);
}

// Test GetAllEntitiesUnderPath with single level directory
TEST_F(HwsFdManagerTest, GetAllEntitiesUnderPath_SingleLevel) {
    auto& manager = HwsFdManager::GetInstance();

    // Open files in /test directory
    auto entity1 = manager.Open("/test/file1.txt", 3001, 0);
    auto entity2 = manager.Open("/test/file2.txt", 3002, 0);

    // Open file in different directory
    auto entity3 = manager.Open("/other/file3.txt", 3003, 0);

    ASSERT_NE(entity1, nullptr);
    ASSERT_NE(entity2, nullptr);
    ASSERT_NE(entity3, nullptr);

    // Get entities under /test
    auto entities = manager.GetAllEntitiesUnderPath("/test");
    EXPECT_EQ(entities.size(), 2);

    // Verify correct files
    std::set<std::string> found_paths;
    for(const auto& entity : entities){
        found_paths.insert(entity->GetPath());
    }

    EXPECT_NE(found_paths.find("/test/file1.txt"), found_paths.end());
    EXPECT_NE(found_paths.find("/test/file2.txt"), found_paths.end());
    EXPECT_EQ(found_paths.find("/other/file3.txt"), found_paths.end());

    // Clean up
    manager.Close(3001);
    manager.Close(3002);
    manager.Close(3003);
}

// Test GetAllEntitiesUnderPath with nested directories
TEST_F(HwsFdManagerTest, GetAllEntitiesUnderPath_NestedDirectories) {
    auto& manager = HwsFdManager::GetInstance();

    // Open files at various levels
    auto entity1 = manager.Open("/test/file1.txt", 4001, 0);
    auto entity2 = manager.Open("/test/subdir/file2.txt", 4002, 0);
    auto entity3 = manager.Open("/test/subdir/deep/file3.txt", 4003, 0);
    auto entity4 = manager.Open("/other/file4.txt", 4004, 0);

    ASSERT_NE(entity1, nullptr);
    ASSERT_NE(entity2, nullptr);
    ASSERT_NE(entity3, nullptr);
    ASSERT_NE(entity4, nullptr);

    // Get entities under /test
    auto entities = manager.GetAllEntitiesUnderPath("/test");
    EXPECT_EQ(entities.size(), 3);

    // Verify nested files are included
    std::set<std::string> found_paths;
    for(const auto& entity : entities){
        found_paths.insert(entity->GetPath());
    }

    EXPECT_NE(found_paths.find("/test/file1.txt"), found_paths.end());
    EXPECT_NE(found_paths.find("/test/subdir/file2.txt"), found_paths.end());
    EXPECT_NE(found_paths.find("/test/subdir/deep/file3.txt"), found_paths.end());
    EXPECT_EQ(found_paths.find("/other/file4.txt"), found_paths.end());

    // Clean up
    manager.Close(4001);
    manager.Close(4002);
    manager.Close(4003);
    manager.Close(4004);
}

// Test GetAllEntitiesUnderPath with trailing slash
TEST_F(HwsFdManagerTest, GetAllEntitiesUnderPath_TrailingSlash) {
    auto& manager = HwsFdManager::GetInstance();

    // Open files
    auto entity1 = manager.Open("/test/file1.txt", 5001, 0);
    auto entity2 = manager.Open("/test/subdir/file2.txt", 5002, 0);

    ASSERT_NE(entity1, nullptr);
    ASSERT_NE(entity2, nullptr);

    // Get entities with trailing slash
    auto entities_with_slash = manager.GetAllEntitiesUnderPath("/test/");
    auto entities_without_slash = manager.GetAllEntitiesUnderPath("/test");

    // Both should return same results
    EXPECT_EQ(entities_with_slash.size(), 2);
    EXPECT_EQ(entities_without_slash.size(), 2);

    // Clean up
    manager.Close(5001);
    manager.Close(5002);
}

// Test GetAllEntitiesUnderPath with invalid path
TEST_F(HwsFdManagerTest, GetAllEntitiesUnderPath_InvalidPath) {
    auto& manager = HwsFdManager::GetInstance();

    // Open a valid file
    auto entity1 = manager.Open("/test/file1.txt", 6001, 0);
    ASSERT_NE(entity1, nullptr);

    // Test with invalid paths
    auto entities_empty = manager.GetAllEntitiesUnderPath("");
    EXPECT_EQ(entities_empty.size(), 0);

    auto entities_relative = manager.GetAllEntitiesUnderPath("test/relative");
    EXPECT_EQ(entities_relative.size(), 0);

    // Clean up
    manager.Close(6001);
}

// Test GetAllEntities after closing files
TEST_F(HwsFdManagerTest, GetAllEntities_AfterClose) {
    auto& manager = HwsFdManager::GetInstance();

    // Open files
    manager.Open("/test/file1.txt", 7001, 0);
    manager.Open("/test/file2.txt", 7002, 0);

    // Verify 2 files open
    auto entities_before = manager.GetAllEntities();
    EXPECT_EQ(entities_before.size(), 2);

    // Close one file
    manager.Close(7001);

    // Verify 1 file remaining
    auto entities_after = manager.GetAllEntities();
    EXPECT_EQ(entities_after.size(), 1);
    EXPECT_EQ(entities_after[0]->GetPath(), "/test/file2.txt");

    // Close second file
    manager.Close(7002);

    // Verify no files remaining
    auto entities_final = manager.GetAllEntities();
    EXPECT_EQ(entities_final.size(), 0);
}

// Test thread safety (basic test)
TEST_F(HwsFdManagerTest, GetAllEntities_ThreadSafe) {
    auto& manager = HwsFdManager::GetInstance();

    // Open multiple files
    for(int i = 0; i < 10; i++){
        std::string path = "/test/file" + std::to_string(i) + ".txt";
        auto entity = manager.Open(path.c_str(), 8000 + i, 0);
        ASSERT_NE(entity, nullptr) << "Failed to open: " << path;
    }

    // Call GetAllEntities multiple times
    auto entities1 = manager.GetAllEntities();
    auto entities2 = manager.GetAllEntities();

    // Both should return same count
    EXPECT_EQ(entities1.size(), 10);
    EXPECT_EQ(entities2.size(), 10);

    // Clean up
    for(int i = 0; i < 10; i++){
        manager.Close(8000 + i);
    }
}

// Main function for standalone test execution
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
