/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#ifndef BTREEINDEX_H
#define BTREEINDEX_H

#include "Bruinbase.h"
#include "PageFile.h"
#include "RecordFile.h"
//Here
#include "BTreeNode.h"
#include <vector>
#include <queue>
#include <iostream>
   
#define RESERVED_PID 0
          
/**
 * The data structure to point to a particular entry at a b+tree leaf node.
 * An IndexCursor consists of pid (PageId of the leaf node) and 
 * eid (the location of the index entry inside the node).
 * IndexCursor is used for index lookup and traversal.
 */
typedef struct {
  // PageId of the index entry
  PageId  pid;  
  // The entry number inside the node
  int     eid;  
} IndexCursor;

/**
 * Implements a B-Tree index for bruinbase.
 * 
 */
class BTreeIndex {
 public:
  BTreeIndex();

  /**
   * Open the index file in read or write mode.
   * Under 'w' mode, the index file should be created if it does not exist.
   * @param indexname[IN] the name of the index file
   * @param mode[IN] 'r' for read, 'w' for write
   * @return error code. 0 if no error
   */
  RC open(const std::string& indexname, char mode);

  /**
   * Close the index file.
   * @return error code. 0 if no error
   */
  RC close();
    
  /**
   * Insert (key, RecordId) pair to the index.
   * @param key[IN] the key for the value inserted into the index
   * @param rid[IN] the RecordId for the record being inserted into the index
   * @return error code. 0 if no error
   */
  RC insert(int key, const RecordId& rid);

  /**
   * Run the standard B+Tree key search algorithm and identify the
   * leaf node where searchKey may exist. If an index entry with
   * searchKey exists in the leaf node, set IndexCursor to its location 
   * (i.e., IndexCursor.pid = PageId of the leaf node, and
   * IndexCursor.eid = the searchKey index entry number.) and return 0. 
   * If not, set IndexCursor.pid = PageId of the leaf node and 
   * IndexCursor.eid = the index entry immediately after the largest 
   * index key that is smaller than searchKey, and return the error 
   * code RC_NO_SUCH_RECORD.
   * Using the returned "IndexCursor", you will have to call readForward()
   * to retrieve the actual (key, rid) pair from the index.
   * @param key[IN] the key to find
   * @param cursor[OUT] the cursor pointing to the index entry with 
   *                    searchKey or immediately behind the largest key 
   *                    smaller than searchKey.
   * @return 0 if searchKey is found. Othewise, an error code
   */
  RC locate(int searchKey, IndexCursor& cursor);

  /**
   * Read the (key, rid) pair at the location specified by the index cursor,
   * and move foward the cursor to the next entry.
   * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
   * @param key[OUT] the key stored at the index cursor location
   * @param rid[OUT] the RecordId stored at the index cursor location
   * @return error code. 0 if no error
   */
  RC readForward(IndexCursor& cursor, int& key, RecordId& rid);


  PageId find(int key, PageId& node_id);

  //helper function
  RC update_root_node()
  {
    RC rc;
    char reserved_area[1024];
    PageId * rootPid_;
    rootPid_ = (PageId *) reserved_area;
    *rootPid_ = rootPid;
    rc = this->pf.write(RESERVED_PID, reserved_area);
    if(rc < 0)
    {
      //std::cerr << std::endl << "BTreeindex::init failed" << std::endl;
    }
    return rc;
  }

  RC getRootPidNow()
  {
    RC rc;
    char reserved_area[1024];
    rc = this->pf.read(RESERVED_PID, reserved_area);
    if(rc < 0)
    {
      // std::cerr << std::endl << "BTreeindex::init failed" << std::endl;
    }
    int * rootPid_;
    rootPid_ = (int *) reserved_area;
    this->rootPid = *rootPid_;
    return rc;
  }

  PageId gimmerootPid()
  {
    return this->rootPid;
  }

  PageFile * getPageFile()
  {
    return &(this->pf);
  }

  RC init();
  //Test

  void printTree() {
    std::queue<PageId> q;
    std::queue<RecordId> leafQ;
    q.push(rootPid);
    // std::cerr << "Our root pid is " << rootPid << std::endl << std::endl;
    // std::cerr << "Size: " << q.size() << std::endl << std::endl;
    // std::cerr << "finish inserting pids" << std::endl;
    while (!q.empty()) {
      PageId current;
      current = q.front();
      //std::cerr << "Front pid is " << current << std::endl << std::endl;
      q.pop();
      //std::cerr << "Post-pop q size is: " << q.size() << std::endl << std::endl;
      BTLeafNode btl;
      BTNonLeafNode btn;
      btl.read(current, pf);
      if(!btl.IsLeafNode())
      {
        btn.read(current, pf);
        btn.printOut();
        std::queue<PageId> pidQ = btn.getPointers();
        while(!pidQ.empty())
        {
          q.push(pidQ.front());
          pidQ.pop();
        }
      }
      else
      {
        btl.printOut();
        std::queue<RecordId> ridQ = btl.getPointers();
        while(!ridQ.empty())
        {
          leafQ.push(ridQ.front());
          ridQ.pop();
        }
      }
    }
  }
  
 private:
  PageFile pf;         /// the PageFile used to store the actual b+tree in disk

  PageId   rootPid;    /// the PageId of the root node
  std::vector<PageId> ancestree;
  /// Note that the content of the above two variables will be gone when
  /// this class is destructed. Make sure to store the values of the two 
  /// variables in disk, so that they can be reconstructed when the index
  /// is opened again later.
};

#endif /* BTREEINDEX_H */
