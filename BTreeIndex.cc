#include "BTreeNode.h"

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf) {
	RC rc;
	//cerr << "Read from leaf node buffer " << &this->buffer << " where pid " << pid << " locates..." << endl;
	rc = pf.read(pid, this->buffer);
	if (rc < 0) {
		cerr << "BTreeindex::read failed because of read pf" << endl;
		return rc;
	}
	char* c = this->buffer;
	l_tuple * leaf_tuple_pointer = (l_tuple * ) c;
	leaf_tuple_pointer += MAX_TUPLE_NUMBER;
	bool* is_it_a_leaf = (bool*) leaf_tuple_pointer;
    this->isLeaf = *is_it_a_leaf;
    //cerr << "Read from leaf node: " << pid << " isLeaf at address: " << &is_it_a_leaf << endl;
    // cerr << "Read from leaf node: " << pid << " isLeaf: " << this->isLeaf << endl;
    is_it_a_leaf++;
    int* count = (int*) is_it_a_leaf;
    this->keyCount = *count;
    //cerr << "Read from leaf node: " << pid << " keyCount at address: " << &count << endl;    
    // cerr << "Read from leaf node: " << pid << " keyCount: " << this->keyCount << endl;
    c = this->buffer;
    c = c + 1020;
    PageId* ppid = (PageId*)c;
    this->sister = *ppid;
    //cerr << "Read from leaf node: " << pid << " sister at address: " << &ppid << endl;    
    // cerr << "Read from leaf node: " << pid << " sister: " << this->sister << endl;
	return 0;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf) {
	RC rc;
	//cerr << "Write to leaf node buffer address " << &this->buffer << " where pid " << pid << " locates..." << endl;
	char* c = this->buffer;
	l_tuple * leaf_tuple_pointer = (l_tuple * ) c;
	leaf_tuple_pointer += MAX_TUPLE_NUMBER;
	bool* is_it_a_leaf = (bool*) leaf_tuple_pointer;
    *is_it_a_leaf = this->isLeaf;
    //cerr << "Write to leaf node: " << pid << " isLeaf from address: " << &is_it_a_leaf << endl;     
    cerr << "Write to leaf node: " << pid << " isLeaf: " << *is_it_a_leaf << endl; 
    is_it_a_leaf++;
    int* count = (int*) is_it_a_leaf;
    *count = this->keyCount;
    //cerr << "Write to leaf node: " << pid << " count from address: " << &count << endl; 
    cerr << "Write to leaf node: " << pid << " count: " << *count << endl; 
    c = this->buffer;
    c = c + 1020;
    PageId* ppid = (PageId*)c;
    *ppid = this->sister;
    //cerr << "Write to leaf node: " << pid << " sister from address: " << &ppid << endl;    
    cerr << "Write to leaf node: " << pid << " sister: " << *ppid << endl; 
	rc = pf.write(pid, this->buffer);
	cerr << "A leaf node with pid " << pid << " is written to the pagefile" << endl;
	cerr << "The key count in that node is now: " << this->keyCount << endl << endl; 
	if (rc < 0) {
		cerr << "BTreeindex::write failed because of write pf" << endl;
		return rc;
	}
	return 0;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount() {
	return this->keyCount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid) {
	if (MAX_TUPLE_NUMBER <= keyCount) {
		cerr << "BTreeindex::insert failed because node is full" << endl;
		return RC_NODE_FULL;
	}
	int tupleIterator = 1;//what l_tuple the tuplePointer points to currently
	char* c;
	int currentKeyValue, nextKeyValue;
	c = this->buffer;
	l_tuple* tuplePointer = (l_tuple*)c;//dynamic_cast<l_tuple*>(c);
	l_tuple temp, temp2;
	if (keyCount < 1)
	{
		tuplePointer->key = key;
		tuplePointer->rid = rid;
		this->keyCount++;
		return 0;
	}
	if (key < tuplePointer->key) {
		temp.key = (tuplePointer+1)->key;
		temp.rid = (tuplePointer+1)->rid;
		(tuplePointer+1)->key = tuplePointer->key;
		(tuplePointer+1)->rid = tuplePointer->rid;
		//To insert the new l_tuple in the front of the buffer
		tuplePointer->key = key;
		tuplePointer->rid = rid;
		tuplePointer++;
		for (int i = 1; i < keyCount - 1; i++) {
			temp2.key = (tuplePointer+1)->key;
			temp2.rid = (tuplePointer+1)->rid;
			(tuplePointer+1)->key = temp.key;
			(tuplePointer+1)->rid = temp.rid;
			temp = temp2;			
			tuplePointer++;
		}
		//To move the last l_tuple in the empty l_tuple holder in the buffer
		tuplePointer++;
		tuplePointer->key = temp.key;
		tuplePointer->rid = temp.rid;
	}
	else {
		while (tupleIterator < keyCount) {
			currentKeyValue = tuplePointer->key;
			nextKeyValue = (tuplePointer+1)->key;
			if (currentKeyValue <= key && nextKeyValue > key) {
				if (tupleIterator < keyCount - 1) {
					tuplePointer = tuplePointer + 1;//Move the pointer to where the l_tuple will be inserted
					temp.key = (tuplePointer+1)->key;
					temp.rid = (tuplePointer+1)->rid;
					(tuplePointer+1)->key = tuplePointer->key;
					(tuplePointer+1)->rid = tuplePointer->rid;
					//To insert the new l_tuple in the right position 
					tuplePointer->key = key;
					tuplePointer->rid = rid;
					tuplePointer++;
					for (int i = 0; i < keyCount - tupleIterator - 2; i++) {
						temp2.key = (tuplePointer+1)->key;
						temp2.rid = (tuplePointer+1)->rid;
						(tuplePointer+1)->key = temp.key;
						(tuplePointer+1)->rid = temp.rid;
						temp = temp2;			
						tuplePointer++;
					}
					//To move the last l_tuple in the empty l_tuple holder in the buffer
					tuplePointer++;
					tuplePointer->key = temp.key;
					tuplePointer->rid = temp.rid;
					keyCount++;
					return 0;
				}
				else {//if we insert to the second to the last position
					//save the last l_tuple in temp
					temp.key = (tuplePointer+1)->key;
					temp.rid = (tuplePointer+1)->rid;
					//To insert the new l_tuple in the right position
					tuplePointer++; 
					tuplePointer->key = key;
					tuplePointer->rid = rid;
					//To move the previous last l_tuple to the end of the buffer
					tuplePointer++;
					tuplePointer->key = temp.key;
					tuplePointer->rid = temp.rid;
				}
			}
			else {
				tupleIterator++;
				tuplePointer++;
			}
		}
		//Go to the next empty l_tuple holder and place the new l_tuple in it
		tuplePointer++;
		tuplePointer->key = key;
		tuplePointer->rid = rid;
	}
	keyCount++;
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey) {
	if (sibling.getKeyCount() != 0) {
		cerr << "BTreeindex::insertAndSplit failed because of getKeyCount" << endl;
		return RC_INVALID_PID;
	}
	//Update keyCount and determine the sibling's keyCount
	//this->keyCount++;
	RC rc;
	// int sizeOfTuple = keyCount / 2;
	// int sizeOfSiblingTuple = keyCount - sizeOfTuple;
	// int numberOfIteration = sizeOfSiblingTuple;
	// char* c;
	// bool toInsert = true;
	// c = this->buffer;
	// l_tuple* tuplePointer = (l_tuple*)c;//dynamic_cast<l_tuple*>(c);
	// int middleTuple = MAX_TUPLE_NUMBER / 2;
	// tuplePointer = tuplePointer + middleTuple;//This is the 43rd l_tuple
	// if (tuplePointer->key < key) {
	// 	tuplePointer++;
	// 	numberOfIteration--;
	// 	rc = sibling.insert(key, rid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert sibling" << endl;
	// 		return rc;
	// 	}
	// 	toInsert = false;
	// }
	// for (int i = 0; i < numberOfIteration; i++) {
	// 	rc = sibling.insert(tuplePointer->key, tuplePointer->rid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
	// 		return rc;
	// 	}
	// 	tuplePointer++;
	// }
	// this->keyCount = toInsert ? sizeOfTuple - 1 : sizeOfTuple;
	// if (toInsert) {
	// 	rc = this->insert(key, rid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
	// 		return rc;
	// 	}
	// }
	// c = sibling.buffer;
	// tuplePointer = (l_tuple*) c;//dynamic_cast<l_tuple*>(c);
	// siblingKey = tuplePointer->key;
	char* c;
	c = this->buffer;
	l_tuple* tuplePointer = (l_tuple*) c;
	int middleTuple = MAX_TUPLE_NUMBER / 2;
	tuplePointer = tuplePointer + middleTuple;
	int totalKeys = this->keyCount;
	if (tuplePointer->key < key) {
		this->keyCount = this->keyCount / 2 + 1;
		int numberOfIteration = totalKeys - this->keyCount;
		tuplePointer = (l_tuple*)this->buffer;
		tuplePointer += this->keyCount;
		for (int i = 0; i < numberOfIteration; i++) {
			rc = sibling.insert(tuplePointer->key, tuplePointer->rid);
			if (rc < 0) {
				cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
				return rc;
			}
			tuplePointer++;
		}
		rc = sibling.insert(key, rid);
		if (rc < 0) {
			cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
			return rc;
		}
	}
	else {
		this->keyCount = this->keyCount / 2;
		int numberOfIteration = totalKeys - this->keyCount;
		tuplePointer = (l_tuple*)this->buffer;
		tuplePointer += this->keyCount;
		for (int i = 0; i < numberOfIteration; i++) {
			rc = sibling.insert(tuplePointer->key, tuplePointer->rid);
			if (rc < 0) {
				cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
				return rc;
			}
			tuplePointer++;			
		}
		rc = this->insert(key, rid);
		if (rc < 0) {
			cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
			return rc;
		}
	}
	c = sibling.buffer;
	tuplePointer = (l_tuple*) c;//dynamic_cast<l_tuple*>(c);
	siblingKey = tuplePointer->key;	
	return 0;
}


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
RC BTLeafNode::locate(int searchKey, int& eid) {
	char* c;
	c = this->buffer;
	l_tuple* tuplePointer = (l_tuple*)c;//dynamic_cast<l_tuple*>(c);
	for(int i = 0; i < this->keyCount; i++){
		if (searchKey == tuplePointer->key) {
			eid = i;
			return 0;
		}
		else if (i == keyCount - 1 && searchKey != tuplePointer->key) {
			eid = keyCount;
			cerr << "BTreeindex::locate failed because no such record" << endl;
			return RC_NO_SUCH_RECORD;
		}
		else if (searchKey < tuplePointer->key && (tuplePointer+1)->key > searchKey) {
			eid = i;
			cerr << "BTreeindex::locate failed because of no such record 2" << endl;
			return RC_NO_SUCH_RECORD;
		}
		else {
			tuplePointer++;
		}
	}
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid) {
	if (eid >= keyCount) {
		cerr << "BTreeindex::readEntry failed because invalid cursor" << endl;
		return RC_INVALID_CURSOR;
	}
	char* c;
	c = this->buffer;
	l_tuple* tuplePointer = (l_tuple*)c;//dynamic_cast<l_tuple*>(c);
	for (int i = 0; i < eid; i++) {
		tuplePointer++;
	}
	key = tuplePointer->key;
	rid = tuplePointer->rid;
	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr() {
	return this->sister;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid) {
	// char* c;
	// c = this->buffer;
	// c = c + 1020;
	// PageId* nodePointer = (PageId*)c;
	// *nodePointer = pid;
	this->sister = pid;
	return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf) {
	RC rc;
	rc = pf.read(pid, this->buffer);
	if (rc < 0) {
		cerr << "BTreeindex::read failed because of read pf" << endl;
		return rc;
	}
	//cerr << "Read from nonleaf node buffer address " << &this->buffer << " where pid " << pid << " locates..." << endl;
	char* c = this->buffer;
	l_tuple * leaf_tuple_pointer = (l_tuple * ) c;
	leaf_tuple_pointer += MAX_TUPLE_NUMBER;
	bool* is_it_a_leaf = (bool*) leaf_tuple_pointer;
    this->isLeaf = *is_it_a_leaf;
    //cerr << "Read nonleaf node: " << pid << " isLeaf at address: " << &is_it_a_leaf << endl;
    // cerr << "Read nonleaf node: " << pid << " isLeaf returns: " << this->isLeaf << endl;
    is_it_a_leaf++;
    int* count = (int*) is_it_a_leaf;
    this->keyCount = *count;
    //cerr << "Read nonleaf node: " << pid << " keyCount at address: " << &count << endl;
    // cerr << "Read nonleaf node: " << pid << " keyCount returns: " << this->keyCount << endl;
    c = this->buffer;
    PageId* ppid = (PageId*)c;
    this->firstPointer = *ppid;
    //cerr << "Read nonleaf node: " << pid << " firstPointer at address: " << &ppid << endl;   
    // cerr << "Read nonleaf node: " << pid << " firstPointer returns: " << this->firstPointer << endl;
    return 0;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf) {
	RC rc;
	//<!--SD
	//cerr << "Write to nonleaf node buffer address " << &this->buffer << " where pid " << pid << " locates..." << endl; 
	char* c = this->buffer;
	l_tuple * leaf_tuple_pointer = (l_tuple * ) c;
	leaf_tuple_pointer += MAX_TUPLE_NUMBER;
	bool* is_it_a_leaf = (bool*) leaf_tuple_pointer;
    *is_it_a_leaf = this->isLeaf;
    //cerr << "Write to Nonleaf node: " << pid << "isLeaf from address: " << &is_it_a_leaf << endl;    
    cerr << "Write to Nonleaf node: " << pid << "isLeaf: " << *is_it_a_leaf << endl;
    is_it_a_leaf++;
    int* count = (int*) is_it_a_leaf;
    *count = this->keyCount;
    //cerr << "Write to Nonleaf node: " << pid << "count from address: " << &count << endl;    
    cerr << "Write to Nonleaf node: " << pid << "count: " << *count << endl;
    c = this->buffer;
    PageId* ppid = (PageId*)c;
    *ppid = this->firstPointer;
    //cerr << "Write to Nonleaf node: " << pid << "firstPointer from address: " << &ppid << endl;   
    cerr << "Write to Nonleaf node: " << pid << "firstPointer: " << *ppid << endl;
    //SD-->
	rc = pf.write(pid, this->buffer);
	cerr << "A non leaf node with pid " << pid << " is written to the pagefile" << endl;
	cerr << "The key count in that node is now: " << this->keyCount << endl << endl; 
	if (rc < 0) {
		cerr << "BTreeindex::write failed because of write pf" << endl;
		return rc;
	}
	return 0;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount(){
	return this->keyCount;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid) {
	if (MAX_TUPLE_NUMBER <= keyCount) {
		cerr << "BTreeindex::insert failed because node is full" << endl;
		return RC_NODE_FULL;
	}
	int tupleIterator = 1;//what nl_tuple the tuplePointer points to currently
	char* c;
	int currentKeyValue, nextKeyValue;
	c = this->buffer;
	//skip the left most page pointer
	PageId* pagePointer = (PageId*)c;
	pagePointer++;
	nl_tuple* tuplePointer = (nl_tuple*)pagePointer;//dynamic_cast<nl_tuple*>(c);
	nl_tuple temp, temp2;
	if (keyCount < 1)
	{
		tuplePointer->key = key;
		tuplePointer->pid = pid;
		this->keyCount++;
		return 0;
	}
	if (key < tuplePointer->key) {
		temp.key = (tuplePointer+1)->key;
		temp.pid = (tuplePointer+1)->pid;
		(tuplePointer+1)->key = tuplePointer->key;
		(tuplePointer+1)->pid = tuplePointer->pid;
		//To insert the new nl_tuple in the front of the buffer
		tuplePointer->key = key;
		tuplePointer->pid = pid;
		tuplePointer++;
		for (int i = 1; i < keyCount - 1; i++) {
			temp2.key = (tuplePointer+1)->key;
			temp2.pid = (tuplePointer+1)->pid;
			(tuplePointer+1)->key = temp.key;
			(tuplePointer+1)->pid = temp.pid;
			temp = temp2;			
			tuplePointer++;
		}
		//To move the last nl_tuple in the empty nl_tuple holder in the buffer
		tuplePointer++;
		tuplePointer->key = temp.key;
		tuplePointer->pid = temp.pid;
	}
	else {
		if (keyCount == 1) {
			(tuplePointer+1)->key = key;
			(tuplePointer+1)->pid = pid;
			keyCount++;
			return 0;
		}
		while (tupleIterator < keyCount) {
			currentKeyValue = tuplePointer->key;
			nextKeyValue = (tuplePointer+1)->key;
			if (currentKeyValue <= key && nextKeyValue > key) {
				if (tupleIterator < keyCount - 1) {
					tuplePointer = tuplePointer + 1;//Move the pointer to where the nl_tuple will be inserted
					temp.key = (tuplePointer+1)->key;
					temp.pid = (tuplePointer+1)->pid;
					(tuplePointer+1)->key = tuplePointer->key;
					(tuplePointer+1)->pid = tuplePointer->pid;
					//To insert the new nl_tuple in the right position 
					tuplePointer->key = key;
					tuplePointer->pid = pid;
					tuplePointer++;
					for (int i = 0; i < keyCount - tupleIterator - 2; i++) {
						temp2.key = (tuplePointer+1)->key;
						temp2.pid = (tuplePointer+1)->pid;
						(tuplePointer+1)->key = temp.key;
						(tuplePointer+1)->pid = temp.pid;
						temp = temp2;			
						tuplePointer++;
					}
					//To move the last nl_tuple in the empty nl_tuple holder in the buffer
					tuplePointer++;
					tuplePointer->key = temp.key;
					tuplePointer->pid = temp.pid;
					keyCount++;
					return 0;
				}
				else {//if we insert to the second to the last position
					//save the last nl_tuple in temp
					temp.key = (tuplePointer+1)->key;
					temp.pid = (tuplePointer+1)->pid;
					//To insert the new nl_tuple in the right position
					tuplePointer++; 
					tuplePointer->key = key;
					tuplePointer->pid = pid;
					//To move the previous last nl_tuple to the end of the buffer
					tuplePointer++;
					tuplePointer->key = temp.key;
					tuplePointer->pid = temp.pid;
				}
			}
			else {
				tupleIterator++;
				tuplePointer++;
			}
		}
		//Go to the next empty nl_tuple holder and place the new nl_tuple in it
		tuplePointer++;
		tuplePointer->key = key;
		tuplePointer->pid = pid;
	}
	keyCount++;
	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey) {
	if (sibling.getKeyCount() != 0) {
		return RC_INVALID_PID;
	}
	//Update keyCount and determine the sibling's keyCount
	// this->keyCount++;
	// RC rc;
	// int sizeOfTuple = keyCount / 2;
	// int sizeOfSiblingTuple = keyCount - sizeOfTuple;
	// int numberOfIteration = sizeOfSiblingTuple;
	// char* c;
	// bool toInsert = true;
	// c = this->buffer;
	// //skip the left most page pointer
	// PageId* pagePointer = (PageId*)c;
	// pagePointer++;
	// nl_tuple* tuplePointer = (nl_tuple*)pagePointer;//dynamic_cast<l_tuple*>(c);
	// int middleTuple = MAX_TUPLE_NUMBER / 2;
	// tuplePointer = tuplePointer + MAX_TUPLE_NUMBER/2;//This is the 43rd l_tuple
	// if (tuplePointer->key < key) {
	// 	tuplePointer++;
	// 	numberOfIteration--;
	// 	rc = sibling.insert(key, pid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert sibling" << endl;
	// 		return rc;
	// 	}
	// 	toInsert = false;
	// }
	// for (int i = 0; i < numberOfIteration; i++) {
	// 	rc = sibling.insert(tuplePointer->key, tuplePointer->pid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert sibling 2" << endl;
	// 		return rc;
	// 	}
	// 	tuplePointer++;
	// }
	// this->keyCount = toInsert ? sizeOfTuple - 1 : sizeOfTuple;
	// if (toInsert) {
	// 	rc = this->insert(key, pid);
	// 	if (rc < 0) {
	// 		cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
	// 		return rc;
	// 	}
	// }
	// c = sibling.buffer;
	// pagePointer = (PageId*) c;
	// pagePointer++;
	// tuplePointer = (nl_tuple*) pagePointer;//dynamic_cast<l_tuple*>(c);
	// midKey = tuplePointer->key;
	// return 0;
	RC rc;
	char * c = this->buffer;
	PageId* pagePointer = (PageId*)c;
	pagePointer++;
	nl_tuple* tuplePointer = (nl_tuple*)pagePointer;
	int middleTuple = MAX_TUPLE_NUMBER / 2;
	int oldTotalSize = this->keyCount;
	tuplePointer += middleTuple;
	if (tuplePointer->key > key) {
		int last_key_in_orig_after_split = (tuplePointer-1)->key;
		PageId last_pageId_in_orig_after_split = (tuplePointer-1)->pid;
		int siblingSize = oldTotalSize / 2;
		for (int i = 0; i < siblingSize; i++) {
			rc = sibling.insert(tuplePointer->key, tuplePointer->pid);
			if (rc < 0) {
				cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
				return rc;
			}
			tuplePointer++;
		}
		this->keyCount = oldTotalSize - siblingSize;
		if (last_key_in_orig_after_split > key) {
			sibling.setFirstPointer(last_pageId_in_orig_after_split);
			rc = this->insert(key, pid);
			if (rc < 0) {
				cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
				return rc;
			}
			this->keyCount--;
			midKey = last_key_in_orig_after_split;
		}
		else {
			midKey = key;
			sibling.setFirstPointer(pid);
		}
	}
	else {
		midKey = tuplePointer->key;
		sibling.setFirstPointer(tuplePointer->pid);
		tuplePointer++;
		int siblingSize = oldTotalSize / 2 - 1;
		for (int i = 0; i < siblingSize - 1; i++) {
			rc = sibling.insert(tuplePointer->key, tuplePointer->pid);
			if (rc < 0) {
				cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
				return rc;
			}
			tuplePointer++;
		}
		rc = sibling.insert(key, pid);
		if (rc < 0) {
			cerr << "BTreeindex::insertAndSplit failed because of insert this" << endl;
			return rc;
		}
		this->keyCount = oldTotalSize - oldTotalSize / 2;
	}
	return 0;

}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid) {
	int i;
	char* c;
	c = this->buffer;
	PageId* pagePointer = (PageId*)c;
	pagePointer++;
	nl_tuple* tuplePointer = (nl_tuple*)pagePointer;//dynamic_cast<l_tuple*>(c);
	for (i = 0; i < keyCount; i++) 
	{
		if (i == 0 && tuplePointer->key > searchKey) {
			char* d;
			d = this->buffer;
			pagePointer = (PageId*)d;
			pid = *pagePointer;
			return 0;
		}
		if (i == keyCount -1) {
			pid = tuplePointer->pid;
			return 0;
		}
		if (tuplePointer->key < searchKey && (tuplePointer + 1)->key <= searchKey) {
			tuplePointer++;
		}
		else if (tuplePointer->key == searchKey) {
			pid = tuplePointer->pid;
			return 0;
		}
		else if (tuplePointer->key < searchKey && (tuplePointer + 1)->key > searchKey) {
			pid = tuplePointer->pid;
			return 0;
		}
		else {
			return -1;
		}
	}
	return -1;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2) {
	// char* c;
	// c = this->buffer;
	// PageId* pagePointer = (PageId*)c;
	// *pagePointer = pid1;
	this->firstPointer = pid1;
	return this->insert(key,pid2); 
}













