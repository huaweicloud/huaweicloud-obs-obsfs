/******************************************************************************
 * snas_list_test.cpp
 *
 * Unit tests for snas_list.h - SNAS linked list and hash table utilities
 *
 * Test coverage:
 * - Double linked list operations (SNAS_ListHead)
 * - Hash list operations (SNAS_HLIST_NODE)
 * - List traversal macros
 * - Edge cases and boundary conditions
 ******************************************************************************/

#include "gtest.h"
#include "snas_list.h"
#include <cstring>
#include <string>

// Test data structure for list operations
typedef struct tagTestNode {
    SNAS_ListHead list;
    int value;
    std::string name;
} TestNode;

// Test data structure for hash list operations
typedef struct tagTestHashNode {
    SNAS_HLIST_NODE hlist;
    int key;
    int data;
} TestHashNode;

// Test fixture for SNAS List operations
class SnasListTest : public ::testing::Test {
protected:
    SNAS_ListHead head;
    TestNode nodes[5];

    void SetUp() override {
        SNAS_InitListHead(&head);
        for (int i = 0; i < 5; i++) {
            nodes[i].value = i;
            nodes[i].name = "node" + std::to_string(i);
        }
    }

    void TearDown() override {
        // Clean up list if needed
    }
};

// Test: SNAS_InitListHead initializes head to point to itself
TEST_F(SnasListTest, InitListHead) {
    SNAS_ListHead test_head;
    SNAS_InitListHead(&test_head);

    EXPECT_EQ(test_head.next, &test_head);
    EXPECT_EQ(test_head.prev, &test_head);
}

// Test: SNAS_ListEmpty returns TRUE for empty list
TEST_F(SnasListTest, ListEmpty) {
    EXPECT_TRUE(SNAS_ListEmpty(&head));

    // Add a node and verify list is not empty
    SNAS_ListAdd(&nodes[0].list, &head);
    EXPECT_FALSE(SNAS_ListEmpty(&head));
}

// Test: SNAS_ListAdd adds element to the head of the list
TEST_F(SnasListTest, ListAdd) {
    SNAS_ListAdd(&nodes[0].list, &head);

    EXPECT_FALSE(SNAS_ListEmpty(&head));
    EXPECT_EQ(head.next, &nodes[0].list);
    EXPECT_EQ(head.prev, &nodes[0].list);
    EXPECT_EQ(nodes[0].list.next, &head);
    EXPECT_EQ(nodes[0].list.prev, &head);

    // Add another node
    SNAS_ListAdd(&nodes[1].list, &head);

    EXPECT_EQ(head.next, &nodes[1].list);
    EXPECT_EQ(nodes[1].list.next, &nodes[0].list);
}

// Test: SNAS_ListAddTail adds element to the tail of the list
TEST_F(SnasListTest, ListAddTail) {
    SNAS_ListAddTail(&nodes[0].list, &head);
    SNAS_ListAddTail(&nodes[1].list, &head);

    EXPECT_EQ(head.next, &nodes[0].list);
    EXPECT_EQ(nodes[0].list.next, &nodes[1].list);
    EXPECT_EQ(nodes[1].list.next, &head);
    EXPECT_EQ(head.prev, &nodes[1].list);
}

// Test: SNAS_ListDel removes element from list
TEST_F(SnasListTest, ListDel) {
    SNAS_ListAdd(&nodes[0].list, &head);
    SNAS_ListAdd(&nodes[1].list, &head);
    SNAS_ListAdd(&nodes[2].list, &head);

    EXPECT_FALSE(SNAS_ListEmpty(&head));

    // Delete middle node (nodes[1])
    SNAS_ListDel(&nodes[1].list);

    EXPECT_EQ(head.next, &nodes[2].list);
    EXPECT_EQ(nodes[2].list.next, &nodes[0].list);
}

// Test: SNAS_ListMove moves a single entry to new list
TEST_F(SnasListTest, ListMove) {
    SNAS_ListHead new_head;
    SNAS_InitListHead(&new_head);

    SNAS_ListAdd(&nodes[0].list, &head);
    SNAS_ListAdd(&nodes[1].list, &head);

    // Verify both nodes are in head list
    EXPECT_EQ(head.next, &nodes[1].list);
    EXPECT_EQ(head.prev, &nodes[0].list);

    // Move nodes[1] from head to new_head
    SNAS_ListMove(&nodes[1].list, &new_head);

    // nodes[1] should now be in new_head
    EXPECT_EQ(new_head.next, &nodes[1].list);
    EXPECT_EQ(new_head.prev, &nodes[1].list);

    // head should only have nodes[0]
    EXPECT_EQ(head.next, &nodes[0].list);
    EXPECT_EQ(head.prev, &nodes[0].list);
}

// Test: SNAS_ListMoveTail moves a single entry to tail of new list
TEST_F(SnasListTest, ListMoveTail) {
    SNAS_ListHead new_head;
    SNAS_InitListHead(&new_head);

    SNAS_ListAdd(&nodes[0].list, &new_head);
    SNAS_ListAdd(&nodes[1].list, &head);

    // Move nodes[1] from head to tail of new_head
    SNAS_ListMoveTail(&nodes[1].list, &new_head);

    // head should be empty now
    EXPECT_TRUE(SNAS_ListEmpty(&head));

    // new_head should have both nodes: nodes[0] at head, nodes[1] at tail
    EXPECT_EQ(new_head.next, &nodes[0].list);
    EXPECT_EQ(new_head.prev, &nodes[1].list);
}

// Test: SNAS_ListDelInit removes and reinitializes list entry
TEST_F(SnasListTest, ListDelInit) {
    SNAS_ListAdd(&nodes[0].list, &head);
    SNAS_ListDelInit(&nodes[0].list);

    // After DelInit, the node should point to itself
    EXPECT_EQ(nodes[0].list.next, &nodes[0].list);
    EXPECT_EQ(nodes[0].list.prev, &nodes[0].list);
}

// Test: SNAS_ListIsTail checks if element is tail
TEST_F(SnasListTest, ListIsTail) {
    SNAS_ListAdd(&nodes[0].list, &head);

    EXPECT_FALSE(SNAS_ListIsTail(&nodes[0].list, &head));
    EXPECT_TRUE(SNAS_ListIsTail(&head, &head));
}

// Test: SNAS_ListForEach traverses all elements
TEST_F(SnasListTest, ListForEach) {
    SNAS_ListAddTail(&nodes[0].list, &head);
    SNAS_ListAddTail(&nodes[1].list, &head);
    SNAS_ListAddTail(&nodes[2].list, &head);

    int count = 0;
    SNAS_ListHead *pos;
    SNAS_ListForEach(pos, &head) {
        if (pos != &head) count++;
    }

    EXPECT_EQ(count, 3);
}

// Test: SNAS_ListForEachSafe allows deletion during traversal
TEST_F(SnasListTest, ListForEachSafe) {
    SNAS_ListAddTail(&nodes[0].list, &head);
    SNAS_ListAddTail(&nodes[1].list, &head);
    SNAS_ListAddTail(&nodes[2].list, &head);

    int count = 0;
    SNAS_ListHead *pos, *n;
    SNAS_ListForEachSafe(pos, n, &head) {
        if (pos != &head) {
            count++;
            SNAS_ListDel(pos);
        }
    }

    EXPECT_EQ(count, 3);
    EXPECT_TRUE(SNAS_ListEmpty(&head));
}

// Test: SNAS_ListForTailEach traverses in reverse
TEST_F(SnasListTest, ListForTailEach) {
    SNAS_ListAddTail(&nodes[0].list, &head);
    SNAS_ListAddTail(&nodes[1].list, &head);
    SNAS_ListAddTail(&nodes[2].list, &head);

    SNAS_ListHead *pos;
    int values[3] = {0};
    int index = 0;

    SNAS_ListForTailEach(pos, &head) {
        if (pos != &head) {
            TestNode *node = SNAS_ListEntry(pos, TestNode, list);
            if (index < 3) {
                values[index++] = node->value;
            }
        }
    }

    // Should traverse in reverse order: 2, 1, 0
    EXPECT_EQ(values[0], 2);
    EXPECT_EQ(values[1], 1);
    EXPECT_EQ(values[2], 0);
}

// Test: SNAS_ListEntry retrieves container structure
TEST_F(SnasListTest, ListEntry) {
    SNAS_ListAdd(&nodes[0].list, &head);

    SNAS_ListHead *pos = head.next;
    TestNode *node = SNAS_ListEntry(pos, TestNode, list);

    EXPECT_EQ(node->value, 0);
    EXPECT_EQ(node->name, "node0");
}

// Test: SNAS_ListAppendList appends one list to another
TEST_F(SnasListTest, ListAppendList) {
    SNAS_ListHead head2;
    SNAS_InitListHead(&head2);

    SNAS_ListAddTail(&nodes[0].list, &head);
    SNAS_ListAddTail(&nodes[1].list, &head);
    SNAS_ListAddTail(&nodes[2].list, &head2);
    SNAS_ListAddTail(&nodes[3].list, &head2);

    // Append head2 to head
    SNAS_ListAppendList(&head, &head2);

    // head should be empty now
    EXPECT_TRUE(SNAS_ListEmpty(&head));

    // head2 should have all 4 nodes
    int count = 0;
    SNAS_ListHead *pos;
    SNAS_ListForEach(pos, &head2) {
        if (pos != &head2) count++;
    }
    EXPECT_EQ(count, 4);
}

// Test: SNAS_InitHlistNode initializes hash list node
TEST(SnasHlistTest, InitHlistNode) {
    SNAS_HLIST_NODE node;
    SNAS_InitHlistNode(&node);

    EXPECT_EQ(node.next, (SNAS_HLIST_NODE*)nullptr);
    EXPECT_EQ(node.pprev, (SNAS_HLIST_NODE**)nullptr);
}

// Test: SNAS_HlistUnhashed checks if node is not in hash list
TEST(SnasHlistTest, HlistUnhashed) {
    SNAS_HLIST_NODE node;
    SNAS_InitHlistNode(&node);

    EXPECT_TRUE(SNAS_HlistUnhashed(&node));
}

// Test: SNAS_HlistEmpty checks if hash list is empty
TEST(SnasHlistTest, HlistEmpty) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    EXPECT_TRUE(SNAS_HlistEmpty(&head));
}

// Test: SNAS_HlistAddHead adds node to hash list head
TEST(SnasHlistTest, HlistAddHead) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node1, node2;
    SNAS_InitHlistNode(&node1);
    SNAS_InitHlistNode(&node2);

    SNAS_HlistAddHead(&node1, &head);
    EXPECT_EQ(head.first, &node1);
    EXPECT_EQ(node1.pprev, &head.first);

    SNAS_HlistAddHead(&node2, &head);
    EXPECT_EQ(head.first, &node2);
    EXPECT_EQ(node2.next, &node1);
}

// Test: SNAS_HlistDel removes node from hash list
TEST(SnasHlistTest, HlistDel) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node1, node2;
    SNAS_InitHlistNode(&node1);
    SNAS_InitHlistNode(&node2);

    SNAS_HlistAddHead(&node1, &head);
    SNAS_HlistAddHead(&node2, &head);

    // Delete node2
    SNAS_HlistDel(&node2);

    EXPECT_EQ(head.first, &node1);
    EXPECT_EQ(node1.next, (SNAS_HLIST_NODE*)nullptr);
}

// Test: SNAS_HlistDelInit removes and reinitializes node
TEST(SnasHlistTest, HlistDelInit) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node;
    SNAS_InitHlistNode(&node);
    SNAS_HlistAddHead(&node, &head);

    SNAS_HlistDelInit(&node);

    // Node should be reinitialized
    EXPECT_TRUE(SNAS_HlistUnhashed(&node));
    EXPECT_EQ(head.first, (SNAS_HLIST_NODE*)nullptr);
}

// Test: SNAS_HlistAddBefore adds node before another
TEST(SnasHlistTest, HlistAddBefore) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node1, node2, node3;
    SNAS_InitHlistNode(&node1);
    SNAS_InitHlistNode(&node2);
    SNAS_InitHlistNode(&node3);

    SNAS_HlistAddHead(&node1, &head);
    // Add node2 before node1 (so node2 becomes first)
    SNAS_HlistAddBefore(&node2, &node1);
    // Add node3 before node2 (so node3 becomes first)
    SNAS_HlistAddBefore(&node3, &node2);

    // Order should be: node3 -> node2 -> node1
    EXPECT_EQ(head.first, &node3);
    EXPECT_EQ(node3.next, &node2);
    EXPECT_EQ(node2.next, &node1);
    EXPECT_EQ(node1.next, (SNAS_HLIST_NODE*)nullptr);
}

// Test: SNAS_HlistAddAfter adds node after another
TEST(SnasHlistTest, HlistAddAfter) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node1, node2, node3;
    SNAS_InitHlistNode(&node1);
    SNAS_InitHlistNode(&node2);
    SNAS_InitHlistNode(&node3);

    SNAS_HlistAddHead(&node1, &head);
    // Add node2 after node1 (so order is node1 -> node2)
    SNAS_HlistAddAfter(&node1, &node2);
    // Add node3 after node2 (so order is node1 -> node2 -> node3)
    SNAS_HlistAddAfter(&node2, &node3);

    // Order should be: node1 -> node2 -> node3
    EXPECT_EQ(head.first, &node1);
    EXPECT_EQ(node1.next, &node2);
    EXPECT_EQ(node2.next, &node3);
    EXPECT_EQ(node3.next, (SNAS_HLIST_NODE*)nullptr);
}

// Test: SNAS_HlistMoveList moves one hash list to another
TEST(SnasHlistTest, HlistMoveList) {
    SNAS_HLIST_HEAD head1, head2;
    SNAS_INITHLISTHEAD(&head1);
    SNAS_INITHLISTHEAD(&head2);

    SNAS_HLIST_NODE node1, node2;
    SNAS_InitHlistNode(&node1);
    SNAS_InitHlistNode(&node2);

    SNAS_HlistAddHead(&node1, &head1);
    SNAS_HlistAddHead(&node2, &head1);

    // head1 has: node2 -> node1 (node2 was added last, so it's first)
    EXPECT_EQ(head1.first, &node2);
    EXPECT_EQ(node2.next, &node1);

    // Move list from head1 to head2
    SNAS_HlistMoveList(&head1, &head2);

    EXPECT_TRUE(SNAS_HlistEmpty(&head1));
    EXPECT_FALSE(SNAS_HlistEmpty(&head2));
    // head2 should have the same order as head1 had
    EXPECT_EQ(head2.first, &node2);
    EXPECT_EQ(node2.next, &node1);
}

// Test: SNAS_HlistForEach traverses all hash list nodes
TEST(SnasHlistTest, HlistForEach) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE nodes[3];
    for (int i = 0; i < 3; i++) {
        SNAS_InitHlistNode(&nodes[i]);
        SNAS_HlistAddHead(&nodes[i], &head);
    }

    int count = 0;
    SNAS_HLIST_NODE *pos;
    SNAS_HlistForEach(pos, &head) {
        count++;
    }

    EXPECT_EQ(count, 3);
}

// Test: SNAS_HlistForEachSafe allows deletion during traversal
TEST(SnasHlistTest, HlistForEachSafe) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE nodes[3];
    for (int i = 0; i < 3; i++) {
        SNAS_InitHlistNode(&nodes[i]);
        SNAS_HlistAddHead(&nodes[i], &head);
    }

    int count = 0;
    SNAS_HLIST_NODE *pos, *n;
    SNAS_HlistForEachSafe(pos, n, &head) {
        count++;
        SNAS_HlistDel(pos);
    }

    EXPECT_EQ(count, 3);
    EXPECT_TRUE(SNAS_HlistEmpty(&head));
}

// Test: SNAS_HlistEntry retrieves container structure
TEST(SnasHlistTest, HlistEntry) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    TestHashNode hash_node;
    hash_node.key = 42;
    hash_node.data = 100;

    SNAS_InitHlistNode(&hash_node.hlist);
    SNAS_HlistAddHead(&hash_node.hlist, &head);

    SNAS_HLIST_NODE *pos = head.first;
    TestHashNode *retrieved = SNAS_HlistEntry(pos, TestHashNode, hlist);

    EXPECT_EQ(retrieved->key, 42);
    EXPECT_EQ(retrieved->data, 100);
}

// Test: snas_container_of macro works correctly
TEST(SnasContainerOfTest, ContainerOf) {
    TestNode node;
    node.value = 123;
    node.name = "test";

    // Get the list member address and use container_of to get back
    SNAS_ListHead *list_ptr = &node.list;
    TestNode *retrieved = snas_container_of(list_ptr, TestNode, list);

    EXPECT_EQ(retrieved->value, 123);
    EXPECT_EQ(retrieved->name, "test");
}

// Test: List operations with multiple nodes
TEST_F(SnasListTest, MultipleNodeOperations) {
    // Add all nodes
    for (int i = 0; i < 5; i++) {
        SNAS_ListAddTail(&nodes[i].list, &head);
    }

    // Count nodes
    int count = 0;
    SNAS_ListHead *pos;
    SNAS_ListForEach(pos, &head) {
        if (pos != &head) count++;
    }
    EXPECT_EQ(count, 5);

    // Remove every other node
    SNAS_ListHead *n;
    bool remove = false;
    SNAS_ListForEachSafe(pos, n, &head) {
        if (pos != &head && remove) {
            SNAS_ListDel(pos);
        }
        remove = !remove;
    }

    // Count remaining nodes
    count = 0;
    SNAS_ListForEach(pos, &head) {
        if (pos != &head) count++;
    }
    EXPECT_EQ(count, 3);
}

// Test: Edge case - single node list
TEST_F(SnasListTest, SingleNodeList) {
    SNAS_ListAdd(&nodes[0].list, &head);

    EXPECT_FALSE(SNAS_ListEmpty(&head));
    EXPECT_EQ(head.next, &nodes[0].list);
    EXPECT_EQ(head.prev, &nodes[0].list);

    SNAS_ListDel(&nodes[0].list);
    EXPECT_TRUE(SNAS_ListEmpty(&head));
}

// Test: Hash list edge case - single node
TEST(SnasHlistTest, SingleNodeHashList) {
    SNAS_HLIST_HEAD head;
    SNAS_INITHLISTHEAD(&head);

    SNAS_HLIST_NODE node;
    SNAS_InitHlistNode(&node);
    SNAS_HlistAddHead(&node, &head);

    EXPECT_FALSE(SNAS_HlistEmpty(&head));
    EXPECT_EQ(head.first, &node);

    SNAS_HlistDel(&node);
    EXPECT_TRUE(SNAS_HlistEmpty(&head));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
