/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 5/28/2008
 */

#ifndef BTREENODE_H
#define BTREENODE_H

#include "RecordFile.h"
#include "PageFile.h"

 //Test
 #include <iostream>
 #include <queue>

#define MAX_TUPLE_NUMBER 84
/**
 * BTLeafNode: The class representing a B+tree leaf node.
 */
struct l_tuple {
    int key;
    RecordId rid;
};

struct nl_tuple {
    int key;
    PageId pid;
};

class BTLeafNode {
  public:
   //Constructor
   BTLeafNode() {
    keyCount = 0;
    isLeaf = true;
    sister = -1;
    //<!--SD
    // char* c;
    // c = this->buffer;
    // l_tuple* tuple = (l_tuple*)c;
    // tuple = tuple + MAX_TUPLE_NUMBER;
    // bool* b = (bool*) tuple;
    // *b = true;
    // SD-->
   } 
   /**
    * Insert the (key, rid) pair to the node.
    * Remember that all keys inside a B+tree node should be kept sorted.
    * @param key[IN] the key to insert
    * @param rid[IN] the RecordId to insert
    * @return 0 if successful. Return an error code if the node is full.
    */
    RC insert(int key, const RecordId& rid);

   /**
    * Insert the (key, rid) pair to the node
    * and split the node half and half with sibling.
    * The first key of the sibling node is returned in siblingKey.
    * Remember that all keys inside a B+tree node should be kept sorted.
    * @param key[IN] the key to insert.
    * @param rid[IN] the RecordId to insert.
    * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
    * @param siblingKey[OUT] the first key in the sibling node after split.
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC insertAndSplit(int key, const RecordId& rid, BTLeafNode& sibling, int& siblingKey);

   /**
    * If searchKey exists in the node, set eid to the index entry
    * with searchKey and return 0. If not, set eid to the index entry
    * immediately after the largest index key that is smaller than searchKey, 
    * and return the error code RC_NO_SUCH_RECORD.
    * Remember that keys inside a B+tree node are always kept sorted.
    * @param searchKey[IN] the key to search for.
    * @param eid[OUT] the index entry number with searchKey or immediately
                      behind the largest key smaller than searchKey.
    * @return 0 if searchKey is found. If not, RC_NO_SEARCH_RECORD.
    */
    RC locate(int searchKey, int& eid);

   /**
    * Read the (key, rid) pair from the eid entry.
    * @param eid[IN] the entry number to read the (key, rid) pair from
    * @param key[OUT] the key from the slot
    * @param rid[OUT] the RecordId from the slot
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC readEntry(int eid, int& key, RecordId& rid);

   /**
    * Return the pid of the next slibling node.
    * @return the PageId of the next sibling node 
    */
    PageId getNextNodePtr();


   /**
    * Set the next slibling node PageId.
    * @param pid[IN] the PageId of the next sibling node 
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC setNextNodePtr(PageId pid);

   /**
    * Return the number of keys stored in the node.
    * @return the number of keys in the node
    */
    int getKeyCount();
 
   /**
    * Read the content of the node from the page pid in the PageFile pf.
    * @param pid[IN] the PageId to read
    * @param pf[IN] PageFile to read from
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC read(PageId pid, const PageFile& pf);
    
   /**
    * Write the content of the node to the page pid in the PageFile pf.
    * @param pid[IN] the PageId to write to
    * @param pf[IN] PageFile to write to
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC write(PageId pid, PageFile& pf);

    //Helper function
    char* bufferPointer() {
        return buffer;
    }

    bool IsLeafNode() {
        return this->isLeaf;
    }

    void init() {
        keyCount = 0;
        isLeaf = true;
        sister = -1;
    }

    int get_sister_pointer()
    {
        return this->sister;
    }

    //TEST
    void printOut() {
        std::cout << "**********************NEW LEAF NODE STARTS HERE******************************" << std::endl;
        char* c;
        c = this->buffer;
        l_tuple* l_tuplePointer = (l_tuple*) c;
        for (int i = 0; i < keyCount; i++) {
            std::cout << "Key: " << l_tuplePointer->key << std::endl << "sid: " << l_tuplePointer->rid.sid << std::endl << "PageID; " << (int)l_tuplePointer->rid.pid << std::endl << std::endl;
            l_tuplePointer++;
        }
        std::cout << "**********************NEW LEAF NODE ENDS HERE********************************" << std::endl;

    }


    std::queue<RecordId> getPointers()
    {
        std::queue<RecordId> ridQ;
        char* c = this->buffer;
        l_tuple * tuple_pointer = (l_tuple *) c;
        for(int i = 0; i < this->keyCount; i++)
        {
            ridQ.push(tuple_pointer->rid);
            tuple_pointer++;
        }
        return ridQ;
    }

    // char buffer[PageFile::PAGE_SIZE];
  private:
   /**
    * The main memory buffer for loading the content of the disk page 
    * that contains the node.
    */
    char buffer[PageFile::PAGE_SIZE];
    int keyCount;
    bool isLeaf;
    PageId sister;
}; 


/**
 * BTNonLeafNode: The class representing a B+tree nonleaf node.
 */
class BTNonLeafNode {
  public:
   //Constructor
    BTNonLeafNode() {
        keyCount = 0;
        isLeaf = false;
        //<!--SD
        // char* c;
        // c = this->buffer;
        // l_tuple* tuple = (l_tuple*)c;
        // tuple = tuple + MAX_TUPLE_NUMBER;
        // bool* b = (bool*) tuple;
        // *b = false;
        //SD-->
    } 
   /**
    * Insert a (key, pid) pair to the node.
    * Remember that all keys inside a B+tree node should be kept sorted.
    * @param key[IN] the key to insert
    * @param pid[IN] the PageId to insert
    * @return 0 if successful. Return an error code if the node is full.
    */
    RC insert(int key, PageId pid);

   /**
    * Insert the (key, pid) pair to the node
    * and split the node half and half with sibling.
    * The sibling node MUST be empty when this function is called.
    * The middle key after the split is returned in midKey.
    * Remember that all keys inside a B+tree node should be kept sorted.
    * @param key[IN] the key to insert
    * @param pid[IN] the PageId to insert
    * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
    * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey);

   /**
    * Given the searchKey, find the child-node pointer to follow and
    * output it in pid.
    * Remember that the keys inside a B+tree node are sorted.
    * @param searchKey[IN] the searchKey that is being looked up.
    * @param pid[OUT] the pointer to the child node to follow.
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC locateChildPtr(int searchKey, PageId& pid);

   /**
    * Initialize the root node with (pid1, key, pid2).
    * @param pid1[IN] the first PageId to insert
    * @param key[IN] the key that should be inserted between the two PageIds
    * @param pid2[IN] the PageId to insert behind the key
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC initializeRoot(PageId pid1, int key, PageId pid2);

   /**
    * Return the number of keys stored in the node.
    * @return the number of keys in the node
    */
    int getKeyCount();

   /**
    * Read the content of the node from the page pid in the PageFile pf.
    * @param pid[IN] the PageId to read
    * @param pf[IN] PageFile to read from
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC read(PageId pid, const PageFile& pf);
    
   /**
    * Write the content of the node to the page pid in the PageFile pf.
    * @param pid[IN] the PageId to write to
    * @param pf[IN] PageFile to write to
    * @return 0 if successful. Return an error code if there is an error.
    */
    RC write(PageId pid, PageFile& pf);

    //Helper function
    char* bufferPointer() {
        return buffer;
    }

    bool IsLeafNode() {
        return this->isLeaf;
    }

    PageId setFirstPointer(PageId newPointer) {
        return this->firstPointer = newPointer;
    }

    PageId getFirstPointer() {
        return this->firstPointer;
    }

    void init() {
        keyCount = 0;
        isLeaf = false;
        firstPointer = -1;
    }


    //TEST
    void printOut() {
        std::cout << "**********************NEW NON-LEAF NODE STARTS HERE******************************" << std::endl;
        char* c;
        c = this->buffer;
        PageId* pagePointer = (PageId*) c;
        std::cout << "First: " << (int)*pagePointer << std::endl;
        pagePointer++;
        nl_tuple* nl_tuplePointer = (nl_tuple*) pagePointer;
        //cout << "After the first, key: " << (int)nl_tuplePointer->key << std::endl << std::endl;
        for (int i = 0; i < keyCount; i++) {
            std::cout << "Key: " << nl_tuplePointer->key << std::endl << "pid: " << (int)nl_tuplePointer->pid << std::endl << std::endl;
            nl_tuplePointer++;
        }
        std::cout << "Key Count: " << keyCount << std::endl;
        std::cout << "***********************NEW NON-LEAF NODE ENDS HERE*******************************" << std::endl;

    }

    std::queue<PageId> getPointers()
    {
        std::queue<PageId> pidQ;
        char* c = this->buffer;
        PageId* p = (PageId*) c;
        pidQ.push(*p);
        p++;
        nl_tuple * tuple_pointer = (nl_tuple * ) p;
        for(int i = 0; i < this->keyCount; i++)
        {
            pidQ.push(tuple_pointer->pid);
            tuple_pointer++;
        }
        return pidQ;
    }

  private:
   /**
    * The main memory buffer for loading the content of the disk page 
    * that contains the node.
    */
    char buffer[PageFile::PAGE_SIZE];
    int keyCount;
    bool isLeaf;
    PageId firstPointer;

}; 

#endif /* BTREENODE_H */
