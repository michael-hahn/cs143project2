/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	//RC rc;
    rootPid = pf.endPid();
    // BTNonLeafNode root;
    // root.init();
    // rc = root.write(rootPid, pf);
    // BTLeafNode firstLeaf;
    // PageId firstPid = pf.endPid();
    // root.setFirstPointer(firstPid);
    // rc = root.write(rootPid, pf);
    // firstLeaf.init();
    // rc = firstLeaf.write(firstPid, pf);
    // cout << rc;
    // int i; 
    // i = 1;
}

RC BTreeIndex::init() {
	//BTNonLeafNode root;
    // root.init();
    RC rc;
    BTLeafNode root;
    root.init();
    rc = root.write(rootPid, pf);
    if (rc < 0)
    {
    	cerr << "BTreeIndex::init failed" << endl;
    	return rc;
    }
    //BTLeafNode firstLeaf;
    //PageId firstPid = pf.endPid();
    //root.setFirstPointer(firstPid);
    //rc = root.write(rootPid, pf);
    //firstLeaf.init();
    //rc = firstLeaf.write(firstPid, pf);
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode) {
	return pf.open(indexname,mode);
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close() {
	return pf.close();
}

PageId BTreeIndex::find(int key, PageId& node_id) {
	RC rc;
	BTLeafNode bottom;
	rc = bottom.read(node_id, this->pf);
	//cout << "Bottom keyCount: " << bottom.getKeyCount() << endl;
	if(rc < 0)
	{
		cerr << "BTreeIndex::find failed because of read" << endl;
		return -1;
	}
	if (bottom.IsLeafNode())
	{
		return node_id;
	}
	BTNonLeafNode not_bottom;
	rc = not_bottom.read(node_id, this->pf);
	//cout << "Not Bottom keyCount: " << not_bottom.getKeyCount() << endl;
	// rc = pf.read(node_id, not_bottom.bufferPointer());
	if (rc < 0) {
		cerr << "BTreeIndex::find failed because of read" << endl;
		return -1;
	}
	ancestree.push_back(node_id);
	PageId pid;
	// char* c;
	// c = not_bottom.bufferPointer();
	// l_tuple* tuple = (l_tuple*)c;
 	// tuple = tuple + MAX_TUPLE_NUMBER;
 	// bool* b = (bool*) tuple;
    if (not_bottom.IsLeafNode() != true) {
		not_bottom.locateChildPtr(key, pid);
		node_id = pid;
		return find(key, node_id);
	}
	return node_id;
}


/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid) {
	cerr << "The rootPid is currently: " << this->rootPid << endl << endl;
	RC rc;
	PageId siblingPid;
	PageId bottom_pid, non_bottom_pid;
	non_bottom_pid = rootPid;
	bottom_pid = find(key, non_bottom_pid);
	cerr << "Find the leaf node to insert " << key << " at " << bottom_pid << endl;
	if (bottom_pid < 0) {
		cerr << "BTreeindex::insert failed because of find" << endl;
		return -1;
	}
	if (bottom_pid == rootPid) {
		cerr << "Root node is leaf node now..." << endl;
		BTLeafNode rootLeaf;
		cerr << "Reading the root node now..." << endl << endl;
		rc = rootLeaf.read(bottom_pid, this->pf);
		cerr << endl << "Read finished..." << endl << endl;
		// rc = pf.read(bottom_pid, rootLeaf.bufferPointer());
		if (rc < 0) {
			cerr << "BTreeindex::insert failed because of read rootLeaf" << endl;
			this->ancestree.clear();
			return rc;
		}
		cerr << "Inserting " << key << " to the root node now..." << endl << endl;
		rc = rootLeaf.insert(key, rid);
		if (rc < 0) {
			cerr << "Split nodes now and try inserting again..." << endl << endl;
			BTLeafNode siblingLeaf;
			int siblingLeafKey;
			rc = rootLeaf.insertAndSplit(key, rid, siblingLeaf, siblingLeafKey);
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of insertAndSplit" << endl;
				this->ancestree.clear();
				return rc;
			}
			siblingPid = pf.endPid();
			cerr << "Give the new sibiling node of the old root leaf node its pid... " << siblingPid << endl;
			PageId nextPointer = rootLeaf.getNextNodePtr();
			rootLeaf.setNextNodePtr(siblingPid);
			siblingLeaf.setNextNodePtr(nextPointer);
			cerr << "Writing to the sibling node " << siblingPid << " of the old root leaf node now..." << endl << endl;
			rc = siblingLeaf.write(siblingPid, pf);
			cerr << "Writing finished..." << endl << endl;
			siblingLeaf.printOut();
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of write siblingLeaf" << endl;
				this->ancestree.clear();
				return rc;
			}
			cerr << "Writing to the old root leaf node " << bottom_pid << " now..." << endl << endl;
			rc = rootLeaf.write(bottom_pid, pf);
			cerr << "Writing finished..." << endl << endl;
			rootLeaf.printOut();
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of write rootLeaf" << endl;
				this->ancestree.clear();
				return rc;
			}
			BTNonLeafNode NewRoot;
			PageId RootPid = pf.endPid();
			cerr << "Create a new nonleaf root node and give its pid " << RootPid << " now..." << endl << endl;
			rootPid = RootPid;
			// rc = pf.read(RootPid, NewRoot.bufferPointer());
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of read pf RootPid" << endl;
				this->ancestree.clear();
				return rc;
			}
			NewRoot.setFirstPointer(bottom_pid);
			cerr << "Insert the nonleaf root node with key " << siblingLeafKey << "and pid " << siblingPid << endl << endl;
			rc = NewRoot.insert(siblingLeafKey, siblingPid);
			cerr << "Insertion finished..." << endl << endl;
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of insert NewRoot" << endl;
				this->ancestree.clear();
				return rc;
			}
			cerr << "Writing to the new nonleaf root node " << RootPid << " now..." << endl << endl;
			rc = NewRoot.write(RootPid, pf);
			cerr << "Writing finished..." << endl << endl;
			NewRoot.printOut();
			this->ancestree.clear();
			return rc;
		}
		cerr << endl << "Insertion finished..." << endl << endl;
		cerr << endl << "Writing the the root leaf node "<< bottom_pid << " now..." << endl << endl;
		rc = rootLeaf.write(bottom_pid, pf);
		cerr << endl << "Writing finished..." << endl << endl;
		this->ancestree.clear();
		rootLeaf.printOut();
		return rc;
	}
	cerr << "Root node is no longer a leaf node now..." << endl << endl;
	ancestree.push_back(bottom_pid);
	BTLeafNode bottom;
	cerr << "Reading the bottom leaf node " << bottom_pid << " to insert into now..." << endl << endl;
	rc = bottom.read(bottom_pid, this->pf);
	cerr << "Reading finished..." << endl << endl;
	// rc = pf.read(bottom_pid, bottom.bufferPointer());
	if (rc < 0) {
		cerr << "BTreeindex::insert failed because of read" << endl;
		this->ancestree.clear();
		return rc;
	}
	cerr << "Inserting the key " << key << "to the leaf pid  "<< bottom_pid << " now..." << endl << endl;
	rc = bottom.insert(key, rid);
	if (rc < 0) {
		cerr << "Try split the leaf node and insert again..." << endl << endl;
		ancestree.pop_back();
		BTLeafNode sibling;
		int siblingKey;
		cerr << "Spliting and inserting to the sibling node now... " << endl << endl;
		rc = bottom.insertAndSplit(key, rid, sibling, siblingKey);
		if (rc < 0) {
			cerr << "BTreeindex::insert failed because of insertAndSplit" << endl;
			this->ancestree.clear();
			return rc;
		}
		siblingPid = pf.endPid();
		cerr << "Sibling node is given pid " << siblingPid << endl << endl;
		ancestree.push_back(siblingPid);
		// char* c = bottom.bufferPointer();
		// c = c + 1020;
		// PageId* siblingPointer = (PageId*) c;

		PageId pointerValue = bottom.getNextNodePtr();
		bottom.setNextNodePtr(siblingPid);
		// *siblingPointer = siblingPid;
		// c = sibling.bufferPointer();
		// c = c + 1020;
		// siblingPointer = (PageId*) c;
		// *siblingPointer = pointerValue;
		sibling.setNextNodePtr(pointerValue);
		cerr << "Writing to the new sibling node " << siblingPid << " now..." << endl << endl;
		rc = sibling.write(siblingPid, pf);
		cerr << "Writing finished..." << endl << endl;
		sibling.printOut();
		if (rc < 0) {
			cerr << "BTreeindex::insert failed because of write sibling" << endl;
			this->ancestree.clear();
			return rc;
		}
		cerr << "Writing to the old bottom node " << bottom_pid << " now..." << endl << endl;
		rc = bottom.write(bottom_pid, pf);
		cerr << "Writing finished..." << endl << endl;
		bottom.printOut();
		if (rc < 0) {
			cerr << "BTreeindex::insert failed because of write bottom" << endl;
			this->ancestree.clear();
			return rc;
		}
		cerr << "Going to the nonleaf nodes now..." << endl << endl;
		cerr << "We will need to go back " << ancestree.size() - 1 << " levels..." << endl << endl;
		int an_s = ancestree.size();
		for (int i = an_s - 1; i > 0; --i) {
			cerr << "Starting the " << i << " level now..." << endl << endl;
			BTNonLeafNode parent;
			PageId loc = ancestree[i - 1];
			cerr << "Reading " << i << " level non leaf node " << loc << " now..." << endl << endl;
			rc = parent.read(loc, this->pf);
			cerr << "Reading finished..." << endl << endl;
			// rc = pf.read(ancestree[i - 1], parent.bufferPointer());
			if (rc < 0) {
				cerr << "BTreeindex::insert failed because of read 2" << endl;
				this->ancestree.clear();
				return rc;
			}
			cerr << "Inserting " << siblingKey << "to this nonleaf node " << loc << " now for indexing..." << endl << endl;
			rc = parent.insert(siblingKey, ancestree[i]);
			if (rc < 0) {
				cerr << "This nonleaf node " << loc << " is full, try split it and insert again..." << endl << endl;
				BTNonLeafNode siblingNonLeaf;
				cerr << "Spliting the node " << loc << " and inserting now..." << endl << endl;
				rc = parent.insertAndSplit(siblingKey, ancestree[i], siblingNonLeaf, siblingKey);
				siblingPid = pf.endPid();
				ancestree.pop_back();
				ancestree.pop_back();
				ancestree.push_back(siblingPid);
				cerr << "The new nonleaf sibling has the pid " << siblingPid << endl << endl;
				cerr << "Writing to the new nonleaf sibling node " << siblingPid << " now..." << endl << endl;
				siblingNonLeaf.write(siblingPid, pf);
				cerr << "Writing finsihed..." << endl << endl;
				siblingNonLeaf.printOut();
				cerr << "Writing to the old nonleaf node " << loc << " now..." << endl << endl;
				rc = parent.write(loc, pf);
				cerr << "Writing finished..." << endl << endl;
				parent.printOut();
				if (rc < 0) {
					cerr << "BTreeindex::insert failed because of write parent" << endl;
					this->ancestree.clear();
					return rc;
				}
				//Root
				if (i == 1) {
					cerr << "Need to create a new root...." << endl << endl;
					BTNonLeafNode newRoot;
					PageId newRootPid;
					newRootPid = pf.endPid();
					cerr << "New root is given pid " << newRootPid << endl << endl;
					// newRoot.read(newRootPid, pf);
					// newRoot.insert(siblingKey, siblingPid);
					// c = newRoot.bufferPointer();
					// PageId* firstPid = (PageId*) c;
					// *firstPid = ancestree[i - 1];
					cerr << "Initializing the new root..." << endl << endl;
					newRoot.initializeRoot(loc, siblingKey, siblingPid);
					cerr << "Writing to the new root node " << newRootPid << " now..." << endl << endl;
					rc = newRoot.write(newRootPid, pf);
					cerr << "Writing finished..." << endl << endl;
					rootPid = newRootPid;
					newRoot.printOut();
					if (rc < 0) {
						cerr << "BTreeindex::insert failed because of write newRoot" << endl;
						this->ancestree.clear();
						return rc;
					}
				}
				cerr << "Preparing to update a level above in the tree now..." << endl << endl;
			}
			else {
				cerr << "Insertion finished..." << endl << endl;
				cerr << "Writing to this nonleaf node " << loc << " now..." << endl << endl;
				rc = parent.write(loc, pf);
				cerr << "Writing finished..." << endl << endl;
				parent.printOut();
				if (rc < 0) {
					cerr << "BTreeindex::insert failed because of write parent 2" << endl;
					this->ancestree.clear();
					return rc;
				}
				ancestree.pop_back();
				cerr << "Updating nonleaf node is done..." << endl << endl;
				break;
			}
		}
	}
	else {
		cerr << "Insertion finished..." << endl << endl;
		cerr << "Writing to the leaf node " << bottom_pid << " now..." << endl << endl;
		rc = bottom.write(bottom_pid, pf);
		cerr << "Writing finished..." << endl << endl;
		bottom.printOut();
		if (rc < 0) {
			cerr << "BTreeindex::insert failed because of write bottom 2" << endl;
			this->ancestree.clear();
			return rc;
		}
	}
	this->ancestree.clear();
	return 0;
}

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
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor) {
	RC rc;
	BTNonLeafNode btn;
	BTLeafNode btl;
	PageId pid;
	rc = btn.read(rootPid, pf);
	if (rc < 0) {
		cerr << "BTreeindex::locate failed because of read btn" << endl;
		return rc;
	}
	bool isLeaf = false;
	while (!isLeaf) {
		rc = btn.locateChildPtr(searchKey, pid);
		if (rc < 0) {
			cerr << "BTreeindex::locate failed because of locateChildPtr" << endl;
			return rc;
		}
		rc = btn.read(pid, pf);
		if (rc < 0) {
			cerr << "BTreeindex::locate failed because of read btn 2" << endl;
			return rc;
		}
		isLeaf = btn.IsLeafNode();
	}
	rc = btl.read(pid, pf);
	if (rc < 0) {
		cerr << "BTreeindex::locate failed because of read btl" << endl;
		return rc;
	}
	rc = btl.locate(searchKey, cursor.eid);
	cursor.pid = pid;
	return rc;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid) {
	RC rc;
	BTLeafNode btl;
	rc = btl.read(cursor.pid, pf);
	if (rc < 0) {
		cerr << "BTreeindex::readForward failed because of read btl" << endl;		
		return rc;
	}
	char* c = btl.bufferPointer();
	l_tuple* tuple = (l_tuple*) c;
	tuple += cursor.eid;
	if (cursor.eid >= btl.getKeyCount()) {
		cerr << "BTreeindex::locate failed because of getKeyCount" << endl;
		return RC_INVALID_CURSOR;
	}
	key = tuple->key;
	rid = tuple->rid;
	if (cursor.eid == btl.getKeyCount() - 1) {
		cursor.pid = btl.getNextNodePtr();
		cursor.eid = 0;
		if (cursor.pid == -1) {
			cerr << "BTreeindex::locate failed because it is end of tree" << endl;
			return RC_END_OF_TREE;
		}
	}
	return 0;
}
