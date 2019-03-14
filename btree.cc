#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

// Added to mirror KeyValuePair structure
KeyPointerPair::KeyPointerPair()
{}


KeyPointerPair::KeyPointerPair(const KEY_T &k, const SIZE_T &p) : 
  key(k), pointer(p)
{}


KeyPointerPair::KeyPointerPair(const KeyPointerPair &rhs) :
  key(rhs.key), pointer(rhs.pointer)
{}


KeyPointerPair::~KeyPointerPair()
{}


KeyPointerPair & KeyPointerPair::operator=(const KeyPointerPair &rhs)
{
  return *( new (this) KeyPointerPair(rhs));
}
// /end added stuff

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
        // OK, so we now have the first key that's larger
        // so we ned to recurse on the ptr immediately previous to 
        // this one, if it exists
        rc=b.GetPtr(offset,ptr);
        if (rc) { return rc; }
        return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	    if (op==BTREE_OP_LOOKUP) { 
	      return b.GetVal(offset,value);
	    } else { 
      	  rc = b.SetVal(offset,value);
          if (rc) {  return rc; }
      	  return b.Serialize(buffercache,node);
	    }
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  KEY_T mrk;
  SIZE_T mrp;
  bool c = false;
  return InsertAtNode(superblock.info.rootnode, key, value, mrk, mrp, c);
}

ERROR_T BTreeIndex::InsertAtNode(SIZE_T &node,
                                 const KEY_T &key,
                                 const VALUE_T &value,
                                 KEY_T &maybe_rhs_key,
                                 SIZE_T &maybe_rhs_ptr,
                                 bool &rhs_created)
{
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T ptr;

    rc = b.Unserialize(buffercache,node);
    if (rc!=ERROR_NOERROR) {
      return rc;
    }

    switch (b.info.nodetype) {

    // LEAF_NODE case first so it's the one to handle empty root case
    // TODO make a new LEAF+ROOT type to handle this case?
    // See several piazza questions on handling first few insertions
    case BTREE_LEAF_NODE:

      // Scan through keys looking for matching value
      for (offset=0;offset<b.info.numkeys;offset++) {
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc;  }
        if (testkey==key) {  return ERROR_CONFLICT;  }
        else if (key<testkey) {
          int maxkeys = b.info.GetNumSlotsAsLeaf() * 2/3; //rounding?
          bool TooFull = (b.info.numkeys >= maxkeys);
            
          KeyValuePair kvp = KeyValuePair(key, value);
          rc = b.InsertKeyVal(offset,kvp); //syntax?
          if (rc) {  return rc; }
          if (TooFull) {
            // we split
            int lhs_numkeys = b.info.numkeys / 2;
            int rhs_numkeys = b.info.numkeys - lhs_numkeys;

            // Create the new rhs node.
            // 1 Copy b's info and data
            BTreeNode *rhs = new BTreeNode(b); //TODO prob doesnt need to be ptr
            // 2 Adjust numkeys, which also invalidates all the data after the
            // first *rhs_numkeys* keys (per piazza @487)
            rhs->info.numkeys = rhs_numkeys;
            // 3 Copy b's rhs data into rhs's data starting from rhs's beginning
            // b_offset is initialized to the point in b where rhs data begins
            // rhs_offset is the point in rhs where we insert each kvp (from b)
            SIZE_T b_offset, rhs_offset;
            for (b_offset=lhs_numkeys, rhs_offset=0;
                 offset<b.info.numkeys;
                 b_offset++, rhs_offset++) {
              KeyValuePair kvp;
              rc = b.GetKeyVal(b_offset,kvp);
              if (rc) {  return rc;  }
              rc = rhs->SetKeyVal(rhs_offset,kvp);
              if (rc) {  return rc;  }
            }

            // Write the rhs's first key into maybe_rhs_key.
            // This is the key to be promoted from the split.
            // It will be inserted into our parent node by our caller (i.e. 
            // the last invocation of InsertAtNode on the call stack).
            rc = rhs->GetKey(0,maybe_rhs_key);
            if (rc) {  return rc; }

            // Allocate a node and write the block number allocated for it
            // into maybe_rhs_ptr. Now when we serialize rhs to this block,
            // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
            // ptr associated with the promoted key.
            rc = AllocateNode(maybe_rhs_ptr);
            if (rc) {  return rc; }

            // Indicate to our caller that we split and created a rhs.
            // Put another way, this signals to our caller that maybe_rhs_key
            // and maybe_rhs_ptr are meaningful and in fact rhs's key and ptr.
            // This ensures they'll be inserted into our parent node.
            // TODO Maybe unnecessary; see if we can implement without this.
            rhs_created = true;

            // Finalize rhs by writing to the buffercache.
            rc = rhs->Serialize(buffercache,maybe_rhs_ptr);
            if (rc) {  return rc; }

            // Per piazza @487, this is enough to turn original node into lhs.
            // Data clearing/overwriting unneeded but maybe useful for debugging
            b.info.numkeys = lhs_numkeys;
          }
          // Finalize b by writing to the buffercache.
          // If no split was necessary, then b is at this point just the
          // original node post-insertion. If a split did occur, then b is
          // the lhs post-split. In either case, it's serialized here.
       	  return b.Serialize(buffercache,node);
        }
      }
      if (b.info.numkeys>0) {
        int maxkeys = b.info.GetNumSlotsAsLeaf() * 2/3; //rounding?
        bool TooFull = (b.info.numkeys >= maxkeys);
          
        KeyValuePair kvp = KeyValuePair(key, value);
        //hopefully numkeys works again instead of an offset
        //that's what dinda did in LookupOrUpdate after the for loop
        rc = b.InsertKeyVal(b.info.numkeys,kvp);
        if (rc) {  return rc; }
        if (TooFull) {
          // we split
          int lhs_numkeys = b.info.numkeys / 2;
          int rhs_numkeys = b.info.numkeys - lhs_numkeys;

          // Create the new rhs node.
          // 1 Copy b's info and data
          BTreeNode *rhs = new BTreeNode(b); //TODO prob doesnt need to be ptr
          // 2 Adjust numkeys, which also invalidates all the data after the
          // first *rhs_numkeys* keys (per piazza @487)
          rhs->info.numkeys = rhs_numkeys;
          // 3 Copy b's rhs data into rhs's data starting from rhs's beginning
          // b_offset is initialized to the point in b where rhs data begins
          // rhs_offset is the point in rhs where we insert each kvp (from b)
          SIZE_T b_offset, rhs_offset;
          for (b_offset=lhs_numkeys, rhs_offset=0;
               offset<b.info.numkeys;
               b_offset++, rhs_offset++) {
            KeyValuePair kvp;
            rc = b.GetKeyVal(b_offset,kvp);
            if (rc) {  return rc;  }
            rc = rhs->SetKeyVal(rhs_offset,kvp);
            if (rc) {  return rc;  }
          }

          // Write the rhs's first key into maybe_rhs_key.
          // This is the key to be promoted from the split.
          // It will be inserted into our parent node by our caller (i.e. 
          // the last invocation of InsertAtNode on the call stack).
          rc = rhs->GetKey(0,maybe_rhs_key);
          if (rc) {  return rc; }

          // Allocate a node and write the block number allocated for it
          // into maybe_rhs_ptr. Now when we serialize rhs to this block,
          // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
          // ptr associated with the promoted key.
          rc = AllocateNode(maybe_rhs_ptr);
          if (rc) {  return rc; }

          // Indicate to our caller that we split and created a rhs.
          // Put another way, this signals to our caller that maybe_rhs_key
          // and maybe_rhs_ptr are meaningful and in fact rhs's key and ptr.
          // This ensures they'll be inserted into our parent node.
          // TODO Maybe unnecessary; see if we can implement without this.
          rhs_created = true;

          // Finalize rhs by writing to the buffercache.
          rc = rhs->Serialize(buffercache,maybe_rhs_ptr);
          if (rc) {  return rc; }

          // Per piazza @487, this is enough to turn original node into lhs.
          // Data clearing/overwriting unneeded but maybe useful for debugging
          b.info.numkeys = lhs_numkeys;
        }
        // Finalize b by writing to the buffercache.
        // If no split was necessary, then b is at this point just the
        // original node post-insertion. If a split did occur, then b is
        // the lhs post-split. In either case, it's serialized here.
       	return b.Serialize(buffercache,node);
      }

      break;
    case BTREE_ROOT_NODE:
      if (b.info.numkeys == 0) {

        // Create new lhs and rhs leaf nodes.
        BTreeNode *lhs = new BTreeNode(b); //TODO prob doesnt need to be ptr
        BTreeNode *rhs = new BTreeNode(b); //TODO prob doesnt need to be ptr

        // Create space and ptrs for new leaves.
        SIZE_T lhs_ptr, rhs_ptr;
        rc = AllocateNode(lhs_ptr);
        if (rc) {  return rc;  }
        rc = AllocateNode(rhs_ptr);
        if (rc) {  return rc;  }

        // Insert key and value into lhs and increment numkeys
        KeyValuePair kvp = KeyValuePair(key,value);
        rc = lhs->SetKeyVal(0,kvp);
        if (rc) {  return rc;  }
        lhs->info.numkeys++;

        // Insert ptr from root to lhs.
        rc = b.SetPtr(0,lhs_ptr);
        if (rc) {  return rc;  }

        // offset and ptr are both SIZE_T so after inserting one
        // ptr, I think we just increment offset
        offset = 1; 

        // Insert key into root as well as ptr to (empty) rhs.
        KeyPointerPair kpp = KeyPointerPair(key,rhs_ptr);
        rc = b.SetKeyPtr(offset,kpp);
        if (rc) {  return rc;  }
        // Increment root numkeys
        b.info.numkeys++;

        // Write the updated root node and 2 created leaf nodes.
        rc = lhs->Serialize(buffercache,lhs_ptr);
        if (rc) {  return rc; }
        rc = rhs->Serialize(buffercache,rhs_ptr);
        if (rc) {  return rc; }
        rc = b.Serialize(buffercache,node);
        return rc;
      }
    case BTREE_INTERIOR_NODE:
      // Scan through key/ptr pairs
      //and recurse if possible
      for (offset=0;offset<b.info.numkeys;offset++) {
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc; }
        if (testkey==key) {  return ERROR_CONFLICT;  }
        else if (key<testkey) {
          // OK, so we now have the first key that's larger
          // so we ned to recurse on the ptr immediately previous to 
          // this one, if it exists
          rc=b.GetPtr(offset,ptr);
          if (rc) { return rc; }
          rc = InsertAtNode(ptr,key,value,maybe_rhs_key,maybe_rhs_ptr,rhs_created);
          if (rc) { return rc; }
          if (rhs_created) {
            KeyPointerPair kpp = KeyPointerPair(maybe_rhs_key, maybe_rhs_ptr);
            rc = b.InsertKeyPtr(offset,kpp); //syntax?
            if (rc) {  return rc; }
            int maxkeys = b.info.GetNumSlotsAsInterior() * 2/3; //rounding?
            bool TooFull = (b.info.numkeys >= maxkeys);
            rhs_created = false;
            if (TooFull) {
              // we split
              int lhs_numkeys = b.info.numkeys / 2;
              int rhs_numkeys = b.info.numkeys - lhs_numkeys;
              // Create the new rhs node.
              // 1 Copy b's info and data
              BTreeNode *rhs = new BTreeNode(b); //TODO prob doesnt need to be ptr
              // 2 Adjust numkeys, which also invalidates all the data after the
              // first *rhs_numkeys* keys (per piazza @487)
              rhs->info.numkeys = rhs_numkeys;
              // 3 Copy b's rhs data into rhs's data starting from rhs's beginning
              // b_offset is initialized to the point in b where rhs data begins
              // rhs_offset is the point in rhs where we insert each kvp (from b)
              SIZE_T b_offset, rhs_offset;
              for (b_offset=lhs_numkeys, rhs_offset=0;
                   offset<b.info.numkeys;
                   b_offset++, rhs_offset++) {
                KeyPointerPair kpp;
                rc = b.GetKeyPtr(b_offset,kpp);
                if (rc) {  return rc;  }
                rc = rhs->SetKeyPtr(rhs_offset,kpp);
                if (rc) {  return rc;  }
              }

              // Write the rhs's first key into maybe_rhs_key.
              // This is the key to be promoted from the split.
              // It will be inserted into our parent node by our caller (i.e. 
              // the last invocation of InsertAtNode on the call stack).
              rc = rhs->GetKey(0,maybe_rhs_key);
              if (rc) {  return rc; }

              // Allocate a node and write the block number allocated for it
              // into maybe_rhs_ptr. Now when we serialize rhs to this block,
              // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
              // ptr associated with the promoted key.
              rc = AllocateNode(maybe_rhs_ptr);
              if (rc) {  return rc; }

              // Indicate to our caller that we split and created a rhs.
              // Put another way, this signals to our caller that maybe_rhs_key
              // and maybe_rhs_ptr are meaningful and in fact rhs's key and ptr.
              // This ensures they'll be inserted into our parent node.
              // TODO Maybe unnecessary; see if we can implement without this.
              rhs_created = true;

              // Finalize rhs by writing to the buffercache.
              rc = rhs->Serialize(buffercache,maybe_rhs_ptr);
              if (rc) {  return rc; }

              // Per piazza @487, this is enough to turn original node into lhs.
              // Data clearing/overwriting unneeded but maybe useful for debugging
              b.info.numkeys = lhs_numkeys;
            }

            // Finalize b by writing to the buffercache.
            // If no split was necessary, then b is at this point just the
            // original node post-insertion. If a split did occur, then b is
            // the lhs post-split. In either case, it's serialized here.
            return b.Serialize(buffercache,node);
          }

          return rc;
       	  
        }
      }
      // if we got here, we need to go to the next pointer, if it exists
      if (b.info.numkeys>0) { 
        rc=b.GetPtr(b.info.numkeys,ptr); //hopefully numkeys is what we want and not some offset
        if (rc) { return rc; }
        rc = InsertAtNode(ptr,key,value,maybe_rhs_key,maybe_rhs_ptr,rhs_created);
        if (rc) { return rc; }
        if (rhs_created) {
          KeyPointerPair kpp = KeyPointerPair(maybe_rhs_key, maybe_rhs_ptr);
          rc = b.InsertKeyPtr(offset,kpp); //syntax?
          if (rc) {  return rc; }
          int maxkeys = b.info.GetNumSlotsAsInterior() * 2/3; //rounding?
          bool TooFull = (b.info.numkeys >= maxkeys);
          rhs_created = false;
          if (TooFull) {
            // we split
            int lhs_numkeys = b.info.numkeys / 2;
            int rhs_numkeys = b.info.numkeys - lhs_numkeys;
            // Create the new rhs node.
            // 1 Copy b's info and data
            BTreeNode *rhs = new BTreeNode(b); //TODO prob doesnt need to be ptr
            // 2 Adjust numkeys, which also invalidates all the data after the
            // first *rhs_numkeys* keys (per piazza @487)
            rhs->info.numkeys = rhs_numkeys;
            // 3 Copy b's rhs data into rhs's data starting from rhs's beginning
            // b_offset is initialized to the point in b where rhs data begins
            // rhs_offset is the point in rhs where we insert each kvp (from b)
            SIZE_T b_offset, rhs_offset;
            for (b_offset=lhs_numkeys, rhs_offset=0;
                 offset<b.info.numkeys;
                 b_offset++, rhs_offset++) {
              KeyPointerPair kpp;
              rc = b.GetKeyPtr(b_offset,kpp);
              if (rc) {  return rc;  }
              rc = rhs->SetKeyPtr(rhs_offset,kpp);
              if (rc) {  return rc;  }
            }

            // Write the rhs's first key into maybe_rhs_key.
            // This is the key to be promoted from the split.
            // It will be inserted into our parent node by our caller (i.e. 
            // the last invocation of InsertAtNode on the call stack).
            rc = rhs->GetKey(0,maybe_rhs_key);
            if (rc) {  return rc; }

            // Allocate a node and write the block number allocated for it
            // into maybe_rhs_ptr. Now when we serialize rhs to this block,
            // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
            // ptr associated with the promoted key.
            rc = AllocateNode(maybe_rhs_ptr);
            if (rc) {  return rc; }

            // Indicate to our caller that we split and created a rhs.
            // Put another way, this signals to our caller that maybe_rhs_key
            // and maybe_rhs_ptr are meaningful and in fact rhs's key and ptr.
            // This ensures they'll be inserted into our parent node.
            // TODO Maybe unnecessary; see if we can implement without this.
            rhs_created = true;

            // Finalize rhs by writing to the buffercache.
            rc = rhs->Serialize(buffercache,maybe_rhs_ptr);
            if (rc) {  return rc; }

            // Per piazza @487, this is enough to turn original node into lhs.
            // Data clearing/overwriting unneeded but maybe useful for debugging
            b.info.numkeys = lhs_numkeys;
          }

          // Finalize b by writing to the buffercache.
          // If no split was necessary, then b is at this point just the
          // original node post-insertion. If a split did occur, then b is
          // the lhs post-split. In either case, it's serialized here.
          return b.Serialize(buffercache,node);
        }
        return rc;
      } else {
        // There are no keys at all on this node, so nowhere to go
        // I think this is only the case of a totally empty tree (i.e. b is an empty root)
        return ERROR_NONEXISTENT;
      }
      break;
    default:
      return ERROR_INSANE;
      break;
    }

  return ERROR_INSANE;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T val = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, val);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




