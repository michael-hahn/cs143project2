/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "RecordFile.h"
#include <cstdio>
//me
 #include "BTreeNode.h"
 #include "BTreeIndex.h"
 #include <iostream>
 #include <stdio.h>
 #include <string.h>
 using namespace std;
//me

//RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)

int main()
{
  //run the SQL engine taking user commands from standard input (console).
  //SqlEngine::run(stdin);
  BTreeIndex bti;
  //PageFile pf;
  // BTNonLeafNode root;
  // BTNonLeafNode btn1;
  // BTNonLeafNode btn2;
  // BTLeafNode btl1;
  // BTLeafNode btl2;
  // RecordId r1;
  // r1.pid = 31;
  // r1.sid = 23;
  //cout << pf.endPid() << endl;
  // root.initializeRoot(0, 1, 1);

  // btl1.insert(3, r1);
  // btl1.insert(4, r1);
  // btl1.insert(10, r1);
  // btl1.insert(8, r1);
  // btl1.insert(46, r1);
  // btl1.setNextNodePtr(5);
  // btn1.insert(3, 3);
  // btn1.insert(4, 4);
  // btn1.insert(10, 10);
  // btn1.insert(8, 8);
  // btn1.insert(46, 46);


  // if (btl1.insert(5, r1) < 0){
  // 	cout << "OVERFLOW!" << endl;
  // }
  // else cout << "SHOULD BE OVERFLOW! BUG BUG BUG" << endl;
  // int midkey;
  // btl1.insertAndSplit(9, r1, btl2, midkey);
  // cout << "The midkey is: " << midkey << endl << endl;
  // cout << "The keycount for node 1 is: " << btl1.getKeyCount() << endl;
  // cout << endl;
  //btn1.printOut();
  // cout << "The sibling node has: " << endl;
  // btl2.printOut();
  // cout << btl1.write(0, pf) << endl << endl;
  // //btl2.write(1, pf);
  // PageFile pf2;
  // cout << "Valid PID: " << pf.endPid() << endl << endl;
  // cout << btl1.read(pf.endPid(), pf) << endl << endl;
  // char * kokomo[1024];
  // cout << pf.read(pf.endPid(), kokomo) << endl << endl;
  // cout << kokomo;
  // cout << endl << endl;
  // cout << btl2.bufferPointer();
  // if(0 == memcmp(kokomo, btl1.buffer, 1024))
  // {
  // 	cout << "Match perfectly!" << endl;
  // }
  // else
  // {
  // 	cout << "FUCK MY LIFE" << endl;
  // }
  /*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
  // RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
 /**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
  // RC BTLeafNode::locate(int searchKey, int& eid)
 //BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
 // int cursor =6;
 // cout << "Locate returned: " << btl1.locate(2,cursor) << endl << "And the cursor is pointing to "; 
 // cout << endl <<endl << cursor << endl;
 // int key;
 // RecordId rid;
 // cout << btl1.readEntry(50, key, rid);
 // cout << "Key is: " << key << endl << endl;
 // cout << "RecordId's Pid is :" << rid.pid << endl << endl;
 // cout << "RecordId's Slot ID is: " << rid.sid << endl << endl;
  //cout << btl1.getNextNodePtr();
 //BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
 // PageId pid;
 // btn1.locateChildPtr(100, pid);
 // cout << pid << endl;
 // BTreeIndex::insert(int key, const RecordId& rid) 
 RecordId rid;
 rid.pid = 12;
 rid.sid = 1;
 bti.open("Test_Index",'w');
 bti.init();
 cerr << "finish open index file" << endl;
 bti.insert(70, rid);
 bti.insert(50, rid);
 bti.insert(20, rid);
 bti.insert(30, rid);
 bti.insert(80, rid);
 bti.insert(90, rid);
 bti.insert(120, rid);
 bti.insert(5, rid);
 bti.insert(25, rid);
 bti.insert(15, rid);
 bti.insert(60, rid);
 bti.insert(1, rid);
 bti.insert(110, rid);
 bti.insert(40, rid);
 bti.insert(35, rid);
 bti.insert(10, rid);
 bti.insert(0, rid);
 cerr << "Ready to print BTI tree!" << endl;
 bti.printTree();
 // bti.insert(60, rid);
 // std::cout << endl << endl; 
 // bti.printTree();
 return 0;
}
