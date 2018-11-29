/******************************************************************************
Copyright (C), 1988-2009, HuaweiSymantec Tech. Co., Ltd.

  �� �� ��   : snas_list.h
  �� �� ��   : ����
  ��    ��   :
  ��������   : 2012��6��18��
  ����޸�   :
  ��������   :
  �����б�   :

  �޸���ʷ   :
  1.��    ��   : 2012��6��18��
    CR  ����   :
    ��    ��   :
    �޸�����   : �����ļ�
******************************************************************************/

#ifndef __SNAS_LIST_H__
#define __SNAS_LIST_H__

#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif

#include <stddef.h>

typedef void            VOID;
typedef bool            BOOL;

typedef unsigned int    U32;

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif


typedef struct tag_SNAS_ListHead
{
    struct tag_SNAS_ListHead *prev;
    struct tag_SNAS_ListHead *next;
} SNAS_ListHead;

#define SNAS_LIST_HEAD_SIZE sizeof(SNAS_ListHead)

/* ����������Ϣ�Ľṹ�� */
typedef struct tagSNASListDesc
{
    SNAS_ListHead stListHead;  /* ָ������ͷ*/
    U32 uiCnt;                 /* �����м�¼�ĸ��� */
    U32 uiPadding;
}SNAS_LIST_DESC_STRU;

/* �����Ӧ�Ĳ����� */
typedef struct tagSNAS_ListOperations
{
    U32  uiMaxLen;
    U32  uiPadding;

    void (*pfLockList)(SNAS_LIST_DESC_STRU *pstDesc);   /* ������� */
    void (*pfUnlockList)(SNAS_LIST_DESC_STRU *pstDesc); /* ������� */
    SNAS_ListHead* (*pfGetList)(U32 *puiCnt);           /* ��ȡ����ͷ */
    BOOL (*pfNodeCmp)(VOID *pKeySrc, VOID *pKeyDes);    /* �ؼ��ֱȽ� */
    void (*pfFreeNode)(VOID *);            /* �����ͷ���������ڵ��ڴ�ĺ��� */
} SNAS_LIST_OPS_STRU;

#define SNAS_ListHeadInit(name) { &(name), &(name) }

static inline VOID __SNAS_ListAdd(SNAS_ListHead * element,
                    SNAS_ListHead * prev,
                    SNAS_ListHead * next)
{
    next->prev = element;
    element->next = next;
    element->prev = prev;
    prev->next = element;
}

static inline VOID SNAS_ListAdd(SNAS_ListHead *element, SNAS_ListHead *head)
{
    __SNAS_ListAdd(element, head, head->next);
}

static inline VOID SNAS_ListAddTail(SNAS_ListHead *element, SNAS_ListHead *head)
{
    __SNAS_ListAdd(element, head->prev, head);
}

static inline VOID __SNAS_ListDel(SNAS_ListHead * prev, SNAS_ListHead * next)
{
    next->prev = prev;
    prev->next = next;
}

static inline VOID SNAS_ListDel(SNAS_ListHead *entry)
{
    __SNAS_ListDel(entry->prev, entry->next);
}

static inline VOID SNAS_ListMove(SNAS_ListHead *list, SNAS_ListHead *head)
{
    __SNAS_ListDel(list->prev, list->next);
    SNAS_ListAdd(list, head);
}

static inline VOID SNAS_ListMoveTail(SNAS_ListHead *list,
                   SNAS_ListHead *head)
{
    __SNAS_ListDel(list->prev, list->next);
    SNAS_ListAddTail(list, head);
}

static inline VOID SNAS_ListMoveHead(SNAS_ListHead *list,
                   SNAS_ListHead *head)
{
    __SNAS_ListDel(list->prev, list->next);
    SNAS_ListAdd(list, head);
}

static inline U32 SNAS_ListEmpty(SNAS_ListHead *head)
{
    return head->next == head;
}

static inline U32 SNAS_ListIsTail(SNAS_ListHead *list, SNAS_ListHead *head)
{
    return list == head;
}

#define SNAS_ListEntry(ptr, type, member) \
     ((type *)((VOID *)((char *)(ptr)-offsetof(type, member))))

#define SNAS_ListTravelFromMember(pos, member, head)\
    for(pos = (member)->next; pos != (head); \
        pos = pos->next)

#define SNAS_ListForEach(pos, head) \
    for (pos = (head)->next; pos != (head); \
        pos = pos->next)

#define SNAS_ListForEachSafe(pos, n, head) \
    for (pos = (head)->next, n = pos->next; pos != (head); \
        pos = n, n = pos->next)

#define SNAS_ListForTailEach(pos, head) \
            for (pos = (head)->prev; pos != (head); \
                pos = pos->prev)

#define SNAS_ListForTailEachSafe(pos, n, head) \
            for (pos = (head)->prev, n = pos->prev; pos != (head); \
                pos = n, n = pos->prev)

#define SNAS_InitListHead(ptr) do { \
    (ptr)->next = (ptr); (ptr)->prev = (ptr); \
} while (0)

static inline VOID SNAS_ListDelInit(SNAS_ListHead *entry)
{
    __SNAS_ListDel(entry->prev, entry->next);
    SNAS_InitListHead(entry);
    return;
}

/* ��һ���б�head_out�е�����Ԫ��(������ͷ���head_out�Լ�)׷�ӵ���һ
   ����head_in��β��������ʼ��head_out */
static inline VOID SNAS_ListAppendList(SNAS_ListHead *head_out,
                   SNAS_ListHead *head_in)
{
    /* head_out������Ԫ��������׷�ӵ���head_in��β�� */
    head_in->prev->next = head_out->next;
    head_out->next->prev = head_in->prev;
    head_in->prev = head_out->prev;
    head_out->prev->next = head_in;

    /* ��ʼ��head_out */
    SNAS_InitListHead(head_out);
}

#define SNAS_ListHeadEntry(ptr, type, member) \
     ((type *)((VOID *)((CHAR *)((ptr)->next) - offsetof(type, member))))

#define SNAS_ListTailEntry(ptr, type, member) \
     ((type *)((VOID *)((CHAR *)((ptr)->prev) - offsetof(type, member))))


/*
 * These are non-NULL pointers that will result in page faults
 * under normal circumstances, used to verify that nobody uses
 * non-initialized list entries.
 */
#define SNAS_LIST_POISON1  ((VOID *) 0x00100100)
#define SNAS_LIST_POISON2  ((VOID *) 0x00200200)

static inline VOID SNAS_Prefetch(VOID *x)
{
    asm volatile("prefetcht0 %0" :: "m" (*(unsigned long *)x));
}

#define SNAS_ListForEachEntry(pos, type, head, member)  \
    for (pos = SNAS_ListEntry((head)->next, type, member),  \
        SNAS_Prefetch(pos->member.next);    \
        &pos->member != (head);    \
        pos = SNAS_ListEntry(pos->member.next, type, member),  \
        SNAS_Prefetch(pos->member.next))

#define SNAS_ListForEachEntrySafe(pos, n, type, head, member)  \
    for (pos = SNAS_ListEntry((head)->next, type, member),  \
         n = SNAS_ListEntry(pos->member.next, type, member); \
         &pos->member != (head);    \
         pos = n, n = SNAS_ListEntry(n->member.next, type, member))

#define SNAS_ListForEachEntrySafeReverse(pos, n, type, head, member) \
    for (pos = SNAS_ListEntry((head)->prev, type, member), \
         n = SNAS_ListEntry(pos->member.prev, type, member); \
         &pos->member != (head); \
         pos = n, n = SNAS_ListEntry(n->member.prev, type, member))

/**
 * container_of - cast a member of a structure out to the containing structure
 * @ptr:    the pointer to the member.
 * @type:   the type of the container struct this is embedded in.
 * @member: the name of the member within the struct.
 *
 */
#define snas_container_of(ptr, type, member) \
    ((type *)((VOID *)((char *)(ptr) - offsetof(type, member))))
/*
do{          \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type,member) );}while(0)
*/
/*
 * Double linked lists with a single pointer list head.
 * Mostly useful for hash tables where the two pointer list head is
 * too wasteful.
 * You lose the ability to access the tail in O(1).
 */

typedef struct tagHlistHead {
    struct tafHlistNode *first;
}SNAS_HLIST_HEAD;

typedef struct tafHlistNode {
    struct tafHlistNode *next, **pprev;
}SNAS_HLIST_NODE;

#define SNAS_HLISTHEAD(name) struct tagHlistHead name = {  .first = NULL }
#define SNAS_INITHLISTHEAD(ptr) ((ptr)->first = NULL)

static inline VOID SNAS_InitHlistNode(SNAS_HLIST_NODE *pstNode)
{
    pstNode->next = NULL;
    pstNode->pprev = NULL;
}

static inline BOOL SNAS_HlistUnhashed(const SNAS_HLIST_NODE *pstNode)
{
    return NULL == pstNode->pprev ? TRUE : FALSE;
}
static inline BOOL SNAS_HlistEmpty(const SNAS_HLIST_HEAD *pstHead)
{
    return NULL == pstHead->first ? TRUE : FALSE;
}

static inline VOID __SNAS_HlistDel(SNAS_HLIST_NODE *pstNode)
{
    SNAS_HLIST_NODE *next = pstNode->next;
    SNAS_HLIST_NODE **pprev = pstNode->pprev;
    *pprev = next;
    if (next)
        next->pprev = pprev;
}

static inline VOID SNAS_HlistDel(SNAS_HLIST_NODE *pstNode)
{
    __SNAS_HlistDel(pstNode);
    pstNode->next = (SNAS_HLIST_NODE *)SNAS_LIST_POISON1;
    pstNode->pprev = (SNAS_HLIST_NODE **)SNAS_LIST_POISON2;
}

static inline VOID SNAS_HlistDelInit(SNAS_HLIST_NODE *pstNode)
{
    if (!SNAS_HlistUnhashed(pstNode)) {
        __SNAS_HlistDel(pstNode);
        SNAS_InitHlistNode(pstNode);
    }
}

static inline VOID SNAS_HlistAddHead(SNAS_HLIST_NODE *pstNode,
                                     SNAS_HLIST_HEAD *pstHead)
{
    SNAS_HLIST_NODE *first = pstHead->first;
    pstNode->next = first;
    if (first)
        first->pprev = &pstNode->next;
    pstHead->first = pstNode;
    pstNode->pprev = &pstHead->first;
}

/* next must be != NULL */
static inline VOID SNAS_HlistAddBefore(SNAS_HLIST_NODE *pstNode,
                    SNAS_HLIST_NODE *pstNext)
{
    pstNode->pprev = pstNext->pprev;
    pstNode->next = pstNext;
    pstNext->pprev = &pstNode->next;
    *(pstNode->pprev) = pstNode;
}

static inline VOID SNAS_HlistAddAfter(SNAS_HLIST_NODE *pstNode,
                    SNAS_HLIST_NODE *pstNext)
{
    pstNext->next = pstNode->next;
    pstNode->next = pstNext;
    pstNext->pprev = &pstNode->next;

    if(pstNext->next)
        pstNext->next->pprev  = &pstNext->next;
}

/*
 * Move a list from one list head to another. Fixup the pprev
 * reference of the first entry if it exists.
 */
static inline VOID SNAS_HlistMoveList(SNAS_HLIST_HEAD *pstOld,
                   SNAS_HLIST_HEAD *pstNew)
{
    pstNew->first = pstOld->first;
    if (pstNew->first)
        pstNew->first->pprev = &pstNew->first;
    pstOld->first = NULL;
}

#define SNAS_HlistEntry(ptr, type, member) snas_container_of(ptr,type,member)

#define SNAS_HlistForEach(pos, head) \
    for (pos = (head)->first; pos; pos = pos->next)

#define SNAS_HlistForEachSafe(pos, n, head) \
    for (pos = (head)->first; pos && ((n = pos->next) || (NULL != head)); pos = n)


/* ���������������ӽڵ㣬��Ҫ�ڵ���ǰ��������� */
#define SNAS_NLADD_NODE(type, pNode, pOps, pDesc, stList, count, rc)        \
do                                                                          \
{                                                                           \
    SNAS_ListHead *pstTmp;                                                  \
    SNAS_ListHead *pstNext;                                                 \
    SNAS_ListHead *pstHead;                                                 \
    type *pstNodeTmp;                                                       \
                                                                            \
    pstHead = (pOps)->pfGetList(NULL);                                      \
                                                                            \
    rc = SNAS_OK;                                                           \
                                                                            \
    if ((pDesc)->uiCnt + 1 <= (pOps)->uiMaxLen)                             \
    {                                                                       \
        /* ��ѯ�Ƿ��Ѿ����� */                                              \
        SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                      \
        {                                                                   \
            pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                     \
                                            type,                           \
                                            stList);                        \
            if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))               \
            {                                                               \
                rc = ERR_BASE_LIST_EXIST;                                   \
                break; /* �˳����� */                                       \
            }                                                               \
        }                                                                   \
        if (ERR_BASE_LIST_EXIST != rc)                                      \
        {                                                                   \
            SNAS_ListAddTail(&(pNode->stList), pstHead);                    \
            (pDesc)->count++;                                               \
        }                                                                   \
    }                                                                       \
    else                                                                    \
    {                                                                       \
        rc = ERR_BASE_LIST_FULL;                                            \
    }                                                                       \
}while(0)

/* ժ��Keyֵָ��������ڵ㣬��������ʹ��ʱ��Ҫ�ڵ���ǰ��ʹ���� */
#define SNAS_NLDEL_NODEBYKEY(type, pOps, pDesc, pNode, stList, count, rc) \
do                                                                        \
{                                                                         \
    SNAS_ListHead *pstTmp;                                                \
    SNAS_ListHead *pstNext;                                               \
    SNAS_ListHead *pstHead;                                               \
    type *pstNodeTmp;                                                     \
                                                                          \
    pstHead = (pOps)->pfGetList(NULL);                                    \
                                                                          \
    rc = ERR_BASE_LIST_NOT_FOUND;                                         \
                                                                          \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                        \
    {                                                                     \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                       \
                                        type,                             \
                                        stList);                          \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))                 \
        {                                                                 \
            SNAS_ListDel(&(pstNodeTmp->stList));                          \
            (pDesc)->count--;                                             \
            pNode = pstNodeTmp;                                           \
            rc = SNAS_OK;                                                 \
            break; /* �˳����� */                                         \
        }                                                                 \
    }                                                                     \
}while(0)

/* ��������ժ��һ���ڵ�, ��pNode����������������Ҫ����ǰ��������� */
#define SNAS_NLDEL_NODE(type, pOps, pDesc, pNode, stList, count)          \
do                                                                        \
{                                                                         \
    SNAS_ListHead *pstTmp;                                                \
    SNAS_ListHead *pstNext;                                               \
    SNAS_ListHead *pstHead;                                               \
    type *pstNodeTmp;                                                     \
                                                                          \
    pstHead = (pOps)->pfGetList(NULL);                                    \
                                                                          \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                        \
    {                                                                     \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                       \
                                        type,                             \
                                        stList);                          \
        SNAS_ListDel(&(pstNodeTmp->stList));                              \
        (pDesc)->count--;                                                 \
        pNode = pstNodeTmp;                                               \
        break; /* �˳����� */                                             \
    }                                                                     \
}while(0)

/* �������ڵ㣬�����԰�����������ʹ��ʱ����������� */
#define SNAS_ADD_NODE(type, pNode, pOps, pDesc, stList, count, rc)         \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = SNAS_OK;                                                          \
                                                                           \
    (pOps)->pfLockList(pDesc);                                             \
    if ((pDesc)->uiCnt + 1 <= (pOps)->uiMaxLen)                            \
    {                                                                      \
        if (NULL != (pOps)->pfNodeCmp)                                     \
        {                                                                  \
             SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                \
             {                                                             \
                 pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,               \
                                                 type,                     \
                                                 stList);                  \
                 if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))         \
                 {                                                         \
                     rc = ERR_BASE_LIST_EXIST;                             \
                     break; /* �˳����� */                                 \
                 }                                                         \
            }                                                              \
        }                                                                  \
        if (ERR_BASE_LIST_EXIST != rc)                                     \
        {                                                                  \
            SNAS_ListAddTail(&(pNode->stList), pstHead);                   \
            (pDesc)->count++;                                              \
        }                                                                  \
    }                                                                      \
    else                                                                   \
    {                                                                      \
        rc = ERR_BASE_LIST_FULL;                                           \
    }                                                                      \
    (pOps)->pfUnlockList(pDesc);                                           \
}while(0)

/* ����ָ���Ľڵ�, ֻ����ָ��, ��û�д�������ժ�� */
#define SNAS_NLFIND_BYKEY(type, pOps, pNode, stList, rc, pFNode)           \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = ERR_BASE_LIST_NOT_FOUND;                                          \
                                                                           \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))                  \
        {                                                                  \
            pFNode = pstNodeTmp;                                           \
            rc = SNAS_OK;                                                  \
            break; /* �˳����� */                                          \
        }                                                                  \
    }                                                                      \
}while(0)

/* ���ҵ��Ľڵ����ݿ�����pNode�� */
#define SNAS_FIND_BYKEY(type, pOps, pDesc, pNode, stList, rc)              \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = ERR_BASE_LIST_NOT_FOUND;                                          \
                                                                           \
    (pOps)->pfLockList(pDesc);                                             \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))                  \
        {                                                                  \
            MPI_MemoryCopy(pNode, pstNodeTmp, sizeof(type));                       \
            rc = SNAS_OK;                                                  \
            break; /* �˳����� */                                          \
        }                                                                  \
    }                                                                      \
    (pOps)->pfUnlockList(pDesc);                                           \
}while(0)

/* ժ��Keyֵָ��������ڵ� */
#define SNAS_DEL_NODEBYKEY(type, pOps, pDesc, pNodeIn, pNodeOut, stList, count, rc)\
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = ERR_BASE_LIST_NOT_FOUND;                                          \
                                                                           \
    (pOps)->pfLockList(pDesc);                                             \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNodeIn))                \
        {                                                                  \
            SNAS_ListDel(&(pstNodeTmp->stList));                           \
            (pDesc)->count--;                                              \
            pNodeOut = pstNodeTmp;                                         \
            rc = SNAS_OK;                                                  \
            break;                                                         \
        }                                                                  \
    }                                                                      \
    (pOps)->pfUnlockList(pDesc);                                           \
}while(0)

/* ��������ժ��һ���ڵ�, ��pNode���� */
#define SNAS_DEL_NODE(type, pOps, pDesc, pNode, stList, count)             \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp = NULL;                                          \
    SNAS_ListHead *pstNext = NULL;                                         \
    SNAS_ListHead *pstHead = NULL;                                         \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
    (pOps)->pfLockList(pDesc);                                             \
                                                                           \
    if (TRUE == SNAS_ListEmpty(pstHead))                                   \
    {                                                                      \
        (pOps)->pfUnlockList(pDesc);                                       \
        (VOID)pstTmp;                                                      \
        (VOID)pstNext;                                                     \
        (VOID)pstHead;                                                     \
        break;                                                             \
    }                                                                      \
                                                                           \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        SNAS_ListDel(&(pstNodeTmp->stList));                               \
        (pDesc)->count--;                                                  \
        pNode = pstNodeTmp;                                                \
        break;                                                             \
    }                                                                      \
    (pOps)->pfUnlockList(pDesc);                                           \
}while(0)

/* ��������ڵ� */
#define SNAS_UPDATE_NODE(type, pNode, pOps, pDesc, stList, rc)             \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = ERR_BASE_LIST_NOT_FOUND;                                          \
                                                                           \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))                  \
        {                                                                  \
            (pOps)->pfLockList(pDesc);                                     \
            MPI_MemoryCopy(&((pNode)->stList), &(pstNodeTmp->stList),              \
                                        sizeof(SNAS_ListHead));            \
            MPI_MemoryCopy(pstNodeTmp, pNode, sizeof(type));                       \
            (pOps)->pfUnlockList(pDesc);                                   \
            rc = SNAS_OK;                                                  \
            break;                                                         \
        }                                                                  \
    }                                                                      \
}while(0)

/* ��������ڵ� ������ */
#define SNAS_NLUPDATE_NODE(type, pNode, pOps, pDesc, stList, rc)           \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    rc = ERR_BASE_LIST_NOT_FOUND;                                          \
                                                                           \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        if (TRUE == (pOps)->pfNodeCmp(pstNodeTmp, pNode))                  \
        {                                                                  \
            MPI_MemoryCopy(&((pNode)->stList), &(pstNodeTmp->stList),              \
                                        sizeof(SNAS_ListHead));            \
            MPI_MemoryCopy(pstNodeTmp, pNode, sizeof(type));                       \
            rc = SNAS_OK;                                                  \
            break;                                                         \
        }                                                                  \
    }                                                                      \
}while(0)

/* ��������ɾ�����нڵ�,���ͷ���Ӧ���ڴ� */
#define SNAS_FREEALL_NODE(type, pOps, pDesc, stList, count)                \
do                                                                         \
{                                                                          \
    SNAS_ListHead *pstTmp;                                                 \
    SNAS_ListHead *pstNext;                                                \
    SNAS_ListHead *pstHead;                                                \
    type *pstNodeTmp;                                                      \
                                                                           \
    pstHead = (pOps)->pfGetList(NULL);                                     \
                                                                           \
    (pOps)->pfLockList(pDesc);                                             \
    SNAS_ListForEachSafe(pstTmp, pstNext, pstHead)                         \
    {                                                                      \
        pstNodeTmp = (type *)SNAS_ListEntry(pstTmp,                        \
                                        type,                              \
                                        stList);                           \
        SNAS_ListDel(&(pstNodeTmp->stList));                               \
        (pDesc)->count--;                                                  \
        (pOps)->pfFreeNode((VOID *)pstNodeTmp);                            \
    }                                                                      \
    (pOps)->pfUnlockList(pDesc);                                           \
}while(0)



#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif

#endif /* __SNAS_LIST_H__ */

