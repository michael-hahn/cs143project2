/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  //Declare variables in the scope of the select function
  
  RC rc;
  bool has_index = false;
  BTreeIndex index_file;
  RecordFile rf;   // RecordFile containing the table
  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }
  //check if we have a B+ tree index
  if ((rc = index_file.open(table + ".idx", 'r')) == 0)
  {
    has_index = true;
  }


  //clean up the conditions//////////////////////////////////////
  vector<SelCond> key_conditions;
  vector<SelCond> value_conditions;
  vector<SelCond> organized_key_conditions;
  vector<SelCond> organized_everything; 
  for(int i = 0; i < cond.size(); i++)
  {
    if(cond[i].attr == 1)
    {
      key_conditions.push_back(cond[i]);
    }
  }

  for(int i = 0; i < cond.size(); i++)
  {
    if(cond[i].attr == 2)
    {
      value_conditions.push_back(cond[i]);
    }
  }

  for(int i = 0; i < key_conditions.size(); i++)
  {
    if (key_conditions[i].comp != SelCond::NE)
    {
      organized_key_conditions.push_back(key_conditions[i]);
    }
  }

  for(int i = 0; i < key_conditions.size(); i++)
  {
    if (key_conditions[i].comp == SelCond::NE)
    {
      organized_key_conditions.push_back(key_conditions[i]);
    }
  } 

  for(int i = 0; i < organized_key_conditions.size(); i++)
  {
      organized_everything.push_back(organized_key_conditions[i]);
  }

  for(int i = 0; i < value_conditions.size(); i++)
  {
      organized_everything.push_back(value_conditions[i]);
  }
  ////////////////////////////////////////////////////////////////

  //check if the condtions contain keys, if not we don't need to read B+ tree
  if (key_conditions.empty())
  {
    has_index = false;
  }
  else //if it does, we can read the tree
  {
    has_index = true;
  }

  if(attr == 4 && key_conditions.empty())
  {
    // struct SelCond {
    //   int attr;     // attribute: 1 - key column,  2 - value column
    //   enum Comparator { EQ, NE, LT, GT, LE, GE } comp;
    //   char* value;  // the value to compare
    // };

    has_index = true;
    SelCond d;
    d.attr = 1;
    d.comp = SelCond::GT;
    d.value = "-1";
    organized_everything.clear();
    organized_everything.push_back(d);
    for(int i = 0; i < organized_key_conditions.size(); i++)
    {
      organized_everything.push_back(organized_key_conditions[i]);
    }

    for(int i = 0; i < value_conditions.size(); i++)
    {
      organized_everything.push_back(value_conditions[i]);
    }
  }

  //////////////////////////////////////////////////////////////////////

  //What we need to do if we read the tree
  if (has_index == true)
  { 
    cerr << endl << endl << "Going to use the index" << endl << endl;
    RecordId r_id;
    IndexCursor ic;
    int key_value;
    IndexCursor leafIterator;
    IndexCursor boundary;
    //PageFile pf;
    string value;
    int count = 0;
    //check the conditions
    key_value = atoi(organized_everything[0].value);
    switch (organized_everything[0].comp)
    {
      case SelCond::EQ:
      {
        cerr << endl << endl << "Using equality condition" << endl << endl;
        bool toPrintThis = true;
        int key_v;
        rc = index_file.locate(key_value, ic);
        if (rc < 0)
        {
          goto next_;//If we can't find the key to equal we don't print anything and exit
        }
        int break_time = organized_everything.size();
        for (int j = 1; j < organized_everything.size(); ++j)
        {
          if(organized_everything[j].attr == 2)
          {
            break_time = j;
            break;
          }
          int key_value_to_compare;
          key_value_to_compare = atoi(organized_everything[j].value);
          switch(organized_everything[j].comp)
          {
            case SelCond::EQ:
            {
              if(key_value != key_value_to_compare)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::NE:
            {
              if(key_value == key_value_to_compare)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::LE:
            {
              if(key_value_to_compare < key_value)
              {
                toPrintThis = false;
              } 
              break;
            }
            case SelCond::LT:
            {
              if(key_value_to_compare <= key_value)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::GE:
            {
              if(key_value_to_compare > key_value)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::GT:
            {
              if(key_value_to_compare >= key_value)
              {
                toPrintThis = false;
              }
              break;
            }
          }
        }
        for (int k = break_time; k < organized_everything.size(); ++k)
        {
          IndexCursor temp = ic;
          index_file.readForward(temp, key_v, r_id);

          rc = rf.read(r_id, key_v, value);
          if(rc < 0)
          {
            cerr << endl << endl << "3) Something went wrong reading from the record file..." << endl << endl;
            cerr << rc << endl << endl;
            cerr << "Here is the rid that caused the problem--> Pid: " << r_id.pid << " Sid: " << r_id.sid << endl << endl;  
          }
          int rv = strcmp(value.c_str(),organized_everything[k].value);
          switch(organized_everything[k].comp)
          {
            //strcmp <0 the first character that does not match has a lower value in ptr1 than in ptr2, 
            //0 the contents of both strings are equal, >0 the first character does not match has a greater value in ptr1 than in ptr2
            case SelCond::EQ:
            {
              if (rv == 0) 
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::NE:
            {
              if(rv != 0)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::LT:
            {
              if(rv <= 0)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::GT:
            {
              if(rv >= 0)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::LE:
            {
              if(rv < 0)
              {
                toPrintThis = false;
              }
              break;
            }
            case SelCond::GE:
            {
              if(rv > 0)
              {
                toPrintThis = false;
              }
              break;
            }
          }
        }
        if (break_time >= organized_everything.size())
        {
            //(1: key, 2: value, 3: *, 4: count(*))
            if(attr == 2 || attr == 3)
            {
              index_file.readForward(ic, key_v, r_id);
              rc = rf.read(r_id, key_v, value);
              if(rc < 0)
              {
                cerr << endl << endl << "4) Something went wrong reading from the record file..." << endl << endl;
                cerr << rc << endl << endl;
                cerr << "Here is the rid that caused the problem--> Pid: " << r_id.pid << " Sid: " << r_id.sid << endl << endl;  
              }
            }
        }
        if (toPrintThis)
        {
          // count++;//add count if we can print this only entry (so count will be 1)
          switch(attr)
          {
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key_v);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key_v, value.c_str());
              break;
            case 4:
              count++;
              break;
          }
        }             
        break;
      }
      case SelCond::NE: //done ... maybe
      {
        cerr << endl << endl << "Using non-equality condition" << endl << endl;
        //If the first condition is key not equal something, we might as well read the whole record, so we no longer read b+ tree
        has_index = false;
        goto next_;
        break;
      }
      //if the first condition is key less than (or less than or euqal to) something, we find the stop entry of B+ tree
      case SelCond::LE:
      case SelCond::LT:
      {
        cerr << endl << endl << "Using less than condition" << endl << endl;
        //try to locate the entry that holds the key value
        rc = index_file.locate(key_value, ic);
        //mark the boundary, this is where we stop reading more leaf nodes
        boundary.pid = ic.pid;
        boundary.eid = ic.eid;
        // cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl;
        // BTLeafNode test_l;
        // test_l.read(boundary.pid, *index_file.getPageFile());
        // l_tuple* test_pointer = (l_tuple*) test_l.bufferPointer();
        // test_pointer+= boundary.eid;
        // cerr << "The boundary key is: " << test_pointer->key;
        // cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl;        
        //load the rootPid
        char reserved_area[1024];
        //find the starting point which is the beginning of the leaf node
        bool break_right_away = false;
        bool isLeaf = false;
        BTNonLeafNode nl_node;
        BTLeafNode l_node;
        //Then we start to iterator to find the beginning of the leaf node
        leafIterator.pid = index_file.gimmerootPid();
        while(!isLeaf)
        {
          nl_node.read(leafIterator.pid, *index_file.getPageFile());
          if (nl_node.IsLeafNode() == true)
          {
            break;
          }
          leafIterator.pid = nl_node.getFirstPointer();
        }
        //When we get out of the while loop, we know that the leafIterator is now pointing at the first leaf node
        leafIterator.eid = 0;
        int break_time = organized_everything.size();
        while(true)
        {
          //We start reading the first leaf node and keep reading the following nodes until we reach boundary entry
          rc = l_node.read(leafIterator.pid, *index_file.getPageFile());
          if(rc < 0)
          {
            cerr << endl << endl << "Reading the first leaf node failed " << endl << endl;
          }
          bool toPrintThis = true;
          //Then we get the key value of the current entry, this is used to compare all the condition regarding the key
          l_tuple * tuples_iterator = (l_tuple *)l_node.bufferPointer();
          tuples_iterator += leafIterator.eid;
          //cerr << endl << "This tuple iterator points to the key: " << tuples_iterator->key << " And the pid: " << tuples_iterator->rid.pid << " sid: " << tuples_iterator->rid.sid << endl << endl;
          int key_v = tuples_iterator->key;
          //Start to compare the current entry's key with the condition
          for (int i = 1; i < organized_everything.size(); ++i)
          {
            //if we reach the condition reagrding value, we break out of this for loop to the next for loop dedicated to value
            if (organized_everything[i].attr == 2)
            {
              break_time = i;
              break;
            }
            //Otherwise we start to compare
            key_value = atoi(organized_everything[i].value);
            if(organized_everything[i].attr == 1)
            {
              switch(organized_everything[i].comp)
              {
                case SelCond::EQ:
                {
                  if (key_v != key_value)
                  {
                    toPrintThis = false;
                  }
                  break;//No print
                }
                case SelCond::NE:
                {
                  if (key_v == key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::LT:
                {
                  if (key_v >= key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::GT:
                {
                  if (key_v <= key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::LE:
                {
                  if (key_v > key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::GE:
                {
                  if (key_v < key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
              }
            }
          }
          //Now we start to compare the current entry with value if there is a value condition
          for (int j = break_time; j < organized_everything.size(); ++j)
          {
            //cerr << endl << "We now comparing values" << endl;
            //cerr << endl << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<< endl << "The leafIterator pid before is: " << leafIterator.pid << endl;
            index_file.readForward(leafIterator, key_v, r_id);
            //cerr << endl << "!%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"<< endl << "The leafIterator pid after is: " << leafIterator.pid << endl;
            rc = rf.read(r_id, key_v, value);
            if(rc < 0)
            {
              cerr << endl << endl << "1) Something went wrong reading from the record file..." << endl << endl;
              cerr << rc << endl << endl;
              cerr << "Here is the rid that caused the problem--> Pid: " << r_id.pid << " Sid: " << r_id.sid << endl << endl;  
            }
            int rv = strcmp(value.c_str(),organized_everything[j].value);
            switch(organized_everything[j].comp)
            {
              //strcmp <0 the first character that does not match has a lower value in ptr1 than in ptr2, 
              //0 the contents of both strings are equal, >0 the first character does not match has a greater value in ptr1 than in ptr2
              case SelCond::EQ:
              {
                if (rv != 0) 
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::NE:
              {
                if(rv == 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::LT:
              {
                if(rv >= 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::GT:
              {
                if(rv <= 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::LE:
              {
                if(rv > 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::GE:
              {
                if(rv < 0)
                {
                  toPrintThis = false;
                }
                break;
              }
            }
          }
          //check when it is time to move to the next node if readForward is not called 
          if (break_time >= organized_everything.size())
          {
            //(1: key, 2: value, 3: *, 4: count(*))
            if(attr == 2 || attr == 3)
            {
              r_id = tuples_iterator->rid;
              rc = rf.read(r_id, key_v, value);
              if(rc < 0)
              {
                cerr << endl << endl << "2) Something went wrong reading from the record file..." << endl << endl;
                cerr << rc << endl << endl;
                cerr << "Here is the rid that caused the problem--> Pid: " << r_id.pid << " Sid: " << r_id.sid << endl << endl; 

              }
            }
            //cerr << "Right now, the key count for this node is: " << l_node.getKeyCount() << " and the entry eid that we are using is " <<
            //leafIterator.eid << endl << endl;
            
          }

          // bool lessthancond;
          // bool lessthanequalcond;
          // lessthancond =
          // print the tuple 
          if(toPrintThis)
          {
            // if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
            //             (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
            //         ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
            //             (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
            // {
            //   count++;
            // }
            switch (attr) 
            {
              case 1:  // SELECT key
              {
                if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
                    ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
                {
                  fprintf(stdout, "%d\n", key_v);
                  break;
                }
              }
              case 2:  // SELECT value
              {
                if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
                    ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
                {
                  fprintf(stdout, "%s\n", value.c_str());
                  break;
                }
              }
              case 3:  // SELECT *
              {
                if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
                    ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
                {
                  fprintf(stdout, "%d '%s'\n", key_v, value.c_str());
                  break;
                }
              }
              case 4:
              {
                if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
                    ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
                        (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
                {
                  count++;
                  break;
                }
              }

            }
          }

          // if (((SelCond::LT == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
          //               (boundary.pid == leafIterator.pid && boundary.eid > leafIterator.eid))) ||
          //           ((SelCond::LE == organized_everything[0].comp) && ((boundary.pid != leafIterator.pid) || 
          //               (boundary.pid == leafIterator.pid && boundary.eid >= leafIterator.eid))))
          // {
          //   count++;
          // }

          if (break_time >= organized_everything.size())
          {
            if(leafIterator.eid < (l_node.getKeyCount()-1))
            {
              leafIterator.eid++;
            }
            else //else just move to the next entry in the same node
            {
              if (leafIterator.pid == boundary.pid && leafIterator.eid == (l_node.getKeyCount()-1))
              {
                break_right_away = true;
              }
              leafIterator.pid = l_node.get_sister_pointer();
              leafIterator.eid = 0; 
            }
            //cerr << "The entry eid that we are using NOW is " << leafIterator.eid << endl << endl;
          }

          if (break_right_away)
          {
            break;
          }
          //check if it is time to break out; i.e. Have we hit the boundary?
          //cerr << endl << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<< endl << "The leafIterator pid is: " << leafIterator.pid << endl;
          if((SelCond::LT == organized_everything[0].comp) && (boundary.pid == leafIterator.pid) && (boundary.eid == leafIterator.eid))
          {
            cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl;
            BTLeafNode test_l;
            test_l.read(boundary.pid, *index_file.getPageFile());
            l_tuple* test_pointer = (l_tuple*) test_l.bufferPointer();
            test_pointer+= boundary.eid;
            cerr << "The boundart pid is: " << boundary.pid << " The boundary key is: " << test_pointer->key << endl;
            test_l.read(leafIterator.pid, *index_file.getPageFile());
            test_pointer = (l_tuple*) test_l.bufferPointer();
            test_pointer+= leafIterator.eid;
            cerr << "The leafIterator pid is: " << leafIterator.pid << " The leafIterator key is: " << test_pointer->key;
            cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl;   
            cerr << endl << "Now the boundary eid is: " << boundary.eid << " and the leafIterator eid is: " << leafIterator.eid << endl;
            //time to leave
            break;
          }
          //if the cursor has gone past the boundary
          else if((SelCond::LE == organized_everything[0].comp) && (boundary.pid == leafIterator.pid && boundary.eid < leafIterator.eid))
          {
            cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl;
            BTLeafNode test_l;
            test_l.read(boundary.pid, *index_file.getPageFile());
            l_tuple* test_pointer = (l_tuple*) test_l.bufferPointer();
            test_pointer+= boundary.eid;
            cerr << "The boundart pid is: " << boundary.pid << " The boundary key is: " << test_pointer->key << endl;
            test_l.read(leafIterator.pid, *index_file.getPageFile());
            test_pointer = (l_tuple*) test_l.bufferPointer();
            test_pointer+= leafIterator.eid;
            cerr << "The leafIterator pid is: " << leafIterator.pid << " The leafIterator key is: " << test_pointer->key;
            cerr << endl << "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" << endl; 
            cerr << endl << "Now the boundary eid is: " << boundary.eid << " and the leafIterator eid is: " << leafIterator.eid << endl;
            //time to leave
            break;
          }
        }
        break;
      }
      case SelCond::GT:
      case SelCond::GE:
      {
        //cerr << endl << endl << "Using greater than condition" << endl << endl;
        //To locate the entry
        rc = index_file.locate(key_value, ic);
        cerr << endl << endl << "We are at page " << ic.pid << " entry number " << ic.eid << endl << endl; 
        BTLeafNode bblt;
        bblt.read(ic.pid, *index_file.getPageFile());
        if (rc == RC_END_OF_TREE && ic.eid == bblt.getKeyCount())
        {
          cerr << endl <<"END OF THE TREE" << endl;
          break;
        }
        if (rc == 0 && SelCond::GT == organized_everything[0].comp)
        {
          int useless_key;
          RecordId useless_rid;
          index_file.readForward(ic, useless_key, useless_rid);
        }
        // boundary.pid = ic.pid;
        // boundary.eid = ic.eid;
        leafIterator = ic;
        bool break_right_away = false;
        BTLeafNode l_node;
        char reserved_area[1024];
        int break_time = organized_everything.size();
        while(true)
        {
          rc = l_node.read(leafIterator.pid, *index_file.getPageFile());
          if(rc < 0)
          {
            cerr << endl << "Reading the first leaf node faield" << endl << endl;
          }
          //cerr << endl << endl << "We are at page " << leafIterator.pid << " entry number " << leafIterator.eid << endl << endl; 
          PageId sister_Pid = l_node.get_sister_pointer();
          int keyCount_thisNode = l_node.getKeyCount();
          bool toPrintThis = true;
          l_tuple * tuples_iterator = (l_tuple*) l_node.bufferPointer();
          tuples_iterator += leafIterator.eid;
          int key_v = tuples_iterator->key;
          for (int i = 1; i < organized_everything.size(); ++i)
          {
            if (organized_everything[i].attr == 2)
            {
              break_time = i;
              break;
            }
            key_value = atoi(organized_everything[i].value);
            if(organized_everything[i].attr == 1)
            {
              switch(organized_everything[i].comp)
              {
                case SelCond::EQ:
                {
                  if (key_v != key_value)
                  {
                    toPrintThis = false;
                  }
                  break;//No print
                }
                case SelCond::NE:
                {
                  if (key_v == key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::LT:
                {
                  if (key_v >= key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::GT:
                {
                  if (key_v <= key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::LE:
                {
                  if (key_v > key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
                case SelCond::GE:
                {
                  if (key_v < key_value)
                  {
                    toPrintThis = false;
                  }
                  break;
                }
              }
            }
          }
          for (int j = break_time; j < organized_everything.size(); ++j)
          {
            index_file.readForward(leafIterator, key_v, r_id);
            rc = rf.read(r_id, key_v, value);
            if (rc < 0)
            {
              ;
            }
            int rv = strcmp(value.c_str(), organized_everything[j].value);
            switch(organized_everything[j].comp)
            {
              case SelCond::EQ:
              {
                if (rv != 0) 
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::NE:
              {
                if(rv == 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::LT:
              {
                if(rv >= 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::GT:
              {
                if(rv <= 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::LE:
              {
                if(rv > 0)
                {
                  toPrintThis = false;
                }
                break;
              }
              case SelCond::GE:
              {
                if(rv < 0)
                {
                  toPrintThis = false;
                }
                break;
              }
            }
          }
          if (break_time >= organized_everything.size())
          {
            if (attr == 2 || attr == 3)
            {
              r_id = tuples_iterator->rid;
              rc = rf.read(r_id, key_v, value);
              if(rc < 0)
              {
                ;
              }
            }
          }
          if(toPrintThis)
          {
            // if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
            // {
            //   count++;
            // }
            switch(attr)
            {
              case 1:
              {
                if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
                {
                  fprintf(stdout, "%d\n", key_v);
                  break;
                }
              }
              case 2:
              {
                if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
                {
                  fprintf(stdout, "%s\n", value.c_str());
                  break;
                }
              }
              case 3:  // SELECT *
              {
                if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
                {
                  fprintf(stdout, "%d '%s'\n", key_v, value.c_str());
                  break;
                }
              }
              case 4:
              {
                if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
                {
                  count++;
                  break;
                }
              }
            }
          }
          // if (!(sister_Pid == -1 && leafIterator.eid >= keyCount_thisNode))
          // {
          //   count++;
          // }

          if (break_time >= organized_everything.size())
          {
            if (leafIterator.eid < (keyCount_thisNode-1))
            {
              leafIterator.eid++;
            }
            else 
            {
              if (sister_Pid == -1 && leafIterator.eid == (keyCount_thisNode - 1))
              {
                break_right_away = true;
              }
              leafIterator.pid = sister_Pid;
              leafIterator.eid = 0;
            }
          }

          if (break_right_away)
          {
            break;
          }

          if (sister_Pid == -1 && leafIterator.eid > (keyCount_thisNode-1))
          {
            break;
          }
        }
        break;
      }
    }

    // move to the next tuple
    next_:
    ;
    if (attr == 4) 
    {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;
  }

  //old code from the function before time
  if (has_index == false)
  {
    cerr << endl << endl << "Not using the index" << endl << endl;
    RecordId   rid;  // record cursor for table scanning
    int    key;
    string value;
    int    count = 0;
    int    diff;


    // scan the table file from the beginning
    rid.pid = rid.sid = 0;
    count = 0;
    while (rid < rf.endRid()) 
    {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) 
      {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) 
      {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) 
        {
          case 1:
              diff = key - atoi(cond[i].value);
            break;
          case 2:
            diff = strcmp(value.c_str(), cond[i].value);
            break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) 
        {
          case SelCond::EQ:
            if (diff != 0) goto next_tuple;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_tuple;
            break;
        case SelCond::GT:
            if (diff <= 0) goto next_tuple;
            break;
        case SelCond::LT:
            if (diff >= 0) goto next_tuple;
            break;
        case SelCond::GE:
            if (diff < 0) goto next_tuple;
            break;
        case SelCond::LE:
            if (diff > 0) goto next_tuple;
            break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) 
      {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }

    // print matching tuple count if "select count(*)"
    if (attr == 4) 
    {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;
  }
  ////////////////////////////////////////

  // close the table file and return
  exit_select:
  rf.close();
  if (has_index)
  {
    rc = index_file.close();
    if (rc < 0)
    {
      return RC_FILE_CLOSE_FAILED;
    }
  }
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RC rc;
  ifstream inputfile;
  BTreeIndex index_file;
  inputfile.open(loadfile.c_str());
  if (!inputfile) 
  {
    //cout << "An error occured while attempting to open the file" << endl;
    return RC_FILE_OPEN_FAILED;
  }
  if (index)
  {
    rc = index_file.open(table + ".idx", 'w');
    if (rc < 0)
    {
      return RC_FILE_OPEN_FAILED;
    } 
    index_file.init();
  }

  char c;
  string line;
  RecordFile rf;
  RecordId rid;
    
  rc = rf.open(table + ".tbl", 'w');
  if (rc < 0) 
  {
    cout << "An error occured when attempting to open the record file" << endl;
    return rc;
  }
    
  while (inputfile.get(c)) {
      while (true) {
          line = line + c;
          inputfile.get(c);
          if (c == '\n') {
              line = line + c;
              break;
          }
      }
      int key;
      string value;
      cerr << endl << "**************" << endl << line << endl << "**************" << endl;
      rc = SqlEngine::parseLoadLine(line, key, value);
      // cout << key << endl;
      // cout << value << endl;
      if (rc < 0) {
        cerr << "parseLoadLine failed" << endl;
        return rc;
      }
      rid = rf.endRid();
      rc = rf.append(key, value, rid);
      if (rc < 0) 
      {
          cerr << "Append failed" << endl;
          return rc;
      }
      if (index)
      {
        rc = index_file.insert(key, rid);
        if (rc < 0)
        {
          cerr << "Insertion failed!" << endl;
        }
      }
      line = "";
  }
  
  if (!inputfile.eof()) {
      return RC_FILE_READ_FAILED;
  }
  inputfile.close();
  rc = index_file.close();
  if (rc < 0)
  {
    return RC_FILE_CLOSE_FAILED;
  }
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
