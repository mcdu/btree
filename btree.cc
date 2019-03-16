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
  ERROR_T rc;
  bool c = false;
  rc = InsertAtNode(superblock.info.rootnode, key, value, mrk, mrp, c);
  if (rc) { return rc; }
  if (c) {
    BTreeNode curr_root, new_root;
    rc = curr_root.Unserialize(buffercache, superblock.info.rootnode);
    if (rc) { return rc; }
    new_root = curr_root;

    new_root.info.numkeys = 1;
    rc = new_root.SetKey(0, mrk);
    if (rc) { return rc; }
    rc = new_root.SetPtr(0, superblock.info.rootnode);
    if (rc) { return rc; }
    rc = new_root.SetPtr(0, mrp);
    if (rc) { return rc; }

    // allocate space for new root on disk
    SIZE_T root_block;
    rc = AllocateNode(root_block);
    if (rc) { return rc; }

    superblock.info.rootnode = root_block;

    rc = new_root.Serialize(buffercache, root_block);
    if (rc) { return rc; }
    
    superblock.info.numkeys++;
    return rc;
  }
}

ERROR_T BTreeIndex::SplitNode(BTreeNode &b,KEY_T &key_to_rhs,SIZE_T &ptr_to_rhs)
{
  ERROR_T rc;
  int lhs_numkeys = b.info.numkeys / 2;
  int rhs_numkeys = b.info.numkeys / 2;
  if (b.info.numkeys % 2 == 0) {  rhs_numkeys--;  }

  BTreeNode rhs = b;
  rhs.info.numkeys = rhs_numkeys;

  // Write the key to be promoted into key_to_rhs.
  // This is the key to be promoted from the split.
  // It will be inserted into our parent node by our caller (i.e. 
  // the last invocation of InsertAtNode on the call stack).
  rc = b.GetKey(lhs_numkeys,key_to_rhs);
  if (rc) {  return rc; }

  // Allocate a node and write the block number allocated for it
  // into ptr_to_rhs. Now when we serialize rhs to this block,
  // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
  // ptr associated with the promoted key.
  rc = AllocateNode(ptr_to_rhs);
  if (rc) {  return rc; }

  // Copy b's rhs data into rhs's data starting from rhs's beginning
  // b_offset is initialized to the point in b where rhs data begins
  // rhs_offset is the point in rhs where we insert each kvp (from b)
  SIZE_T b_offset, rhs_offset;
  for (rhs_offset=0, b_offset=lhs_numkeys+1;
       rhs_offset<rhs.info.numkeys;
       rhs_offset++, b_offset++) {
    
    SIZE_T copied_ptr;
    KEY_T copied_key;

    rc = b.GetPtr(b_offset,copied_ptr);
    if (rc) {  return rc;  }
    rc = rhs.SetPtr(rhs_offset,copied_ptr);
    if (rc) {  return rc;  }

    rc = b.GetKey(b_offset,copied_key);
    if (rc) {  return rc;  }
    rc = rhs.SetKey(rhs_offset,copied_key);
    if (rc) {  return rc;  }
  }

  SIZE_T copied_ptr;
  // Copy the last pointer
  rc = b.GetPtr(b_offset,copied_ptr);
  if (rc) {  return rc;  }
  rc = rhs.SetPtr(rhs_offset,copied_ptr);
  if (rc) {  return rc;  }

  // Finalize rhs by writing to the buffercache.
  // No need to serialize b (our new lhs) here since our caller will.
  rc = rhs.Serialize(buffercache,ptr_to_rhs);
  if (rc) {  return rc; }

  // Per piazza @487, this is enough to turn original node into lhs.
  // Data clearing/overwriting unneeded but maybe useful for debugging
  b.info.numkeys = lhs_numkeys;
}

ERROR_T BTreeIndex::SplitLeaf(BTreeNode &b,KEY_T &key_to_rhs,SIZE_T &ptr_to_rhs)
{
  ERROR_T rc;
  int lhs_numkeys = b.info.numkeys / 2;
  int rhs_numkeys = b.info.numkeys / 2;
  if (b.info.numkeys % 2 == 1) {  rhs_numkeys++;  }

  BTreeNode rhs = b;
  rhs.info.numkeys = rhs_numkeys;

  // Write the key to be promoted into key_to_rhs.
  // This is the key to be promoted from the split.
  // It will be inserted into our parent node by our caller (i.e. 
  // the last invocation of InsertAtNode on the call stack).
  rc = b.GetKey(lhs_numkeys,key_to_rhs);
  if (rc) {  return rc; }

  // Allocate a node and write the block number allocated for it
  // into ptr_to_rhs. Now when we serialize rhs to this block,
  // maybe_rhs_ptr will be the ptr to it. Thus, maybe_rhs_ptr is the
  // ptr associated with the promoted key.
  rc = AllocateNode(ptr_to_rhs);
  if (rc) {  return rc; }

  // Copy b's rhs data into rhs's data starting from rhs's beginning
  // b_offset is initialized to the point in b where rhs data begins
  // rhs_offset is the point in rhs where we insert each kvp (from b)
  SIZE_T b_offset, rhs_offset;
  for (rhs_offset=0, b_offset=lhs_numkeys;
       rhs_offset<rhs.info.numkeys;
       rhs_offset++, b_offset++) {
    
    KeyValuePair copied_kvp;
    rc = b.GetKeyVal(b_offset,copied_kvp);
    if (rc) {  return rc;  }
    rc = rhs.SetKeyVal(rhs_offset,copied_kvp);
    if (rc) {  return rc;  }
  }

  // Copy the first pointer (does nothing right now)
  SIZE_T copied_ptr;
  rc = b.GetPtr(0,copied_ptr);
  if (rc) {  return rc;  }
  rc = rhs.SetPtr(0,copied_ptr);
  if (rc) {  return rc;  }

  // Finalize rhs by writing to the buffercache.
  // No need to serialize b (our new lhs) here since our caller will.
  rc = rhs.Serialize(buffercache,ptr_to_rhs);
  if (rc) {  return rc; }

  // Per piazza @487, this is enough to turn original node into lhs.
  // Data clearing/overwriting unneeded but maybe useful for debugging
  b.info.numkeys = lhs_numkeys;
}

// Explain params here
// rhs_created:
// Indicate to our caller that we split and created a rhs.
// Put another way, this signals to our caller that maybe_rhs_key
// and maybe_rhs_ptr are meaningful and in fact rhs's key and ptr.
// This ensures they'll be inserted into our parent node.
ERROR_T BTreeIndex::InsertAtNode(const SIZE_T &node,
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
    if (rc) { return rc; }

    switch (b.info.nodetype) {
    case BTREE_ROOT_NODE:
      if (b.info.numkeys == 0) {

        // Create new lhs leaf node and insert first key value pair into it.
        BTreeNode lhs = BTreeNode(BTREE_LEAF_NODE,
                                  b.info.keysize,
                                  b.info.valuesize,
                                  b.info.blocksize);

        SIZE_T lhs_empty_ptr, rhs_empty_ptr;
        rc = lhs.SetPtr(0,lhs_empty_ptr);
        if (rc) {  return rc;  }

        KeyValuePair kvp = KeyValuePair(key,value);
        rc = lhs.InsertKeyVal(0,kvp);
        if (rc) {  return rc;  }

        // Create new rhs leaf node and leave it empty.
        BTreeNode rhs = BTreeNode(BTREE_LEAF_NODE,
                                  b.info.keysize,
                                  b.info.valuesize,
                                  b.info.blocksize);

        rc = rhs.SetPtr(0,rhs_empty_ptr);
        if (rc) {  return rc;  }

        // Allocate space and assign ptrs for new lhs and rhs leaf nodes.
        SIZE_T lhs_ptr, rhs_ptr;
        rc = AllocateNode(lhs_ptr);
        if (rc) {  return rc;  }
        rc = AllocateNode(rhs_ptr);
        if (rc) {  return rc;  }

        // Insert key into root.
        b.info.numkeys++; // can't insert ptrs without this line either
        rc = b.SetKey(0, key);
        if (rc) {  return rc;  }

        // Insert ptr to lhs into root.
        rc = b.SetPtr(0,lhs_ptr);
        if (rc) {  return rc;  }

        // Insert ptr to rhs into root.
        rc = b.SetPtr(1, rhs_ptr);
        if (rc) {  return rc;  }

        // Write the updated root node and 2 created leaf nodes.
        rc = b.Serialize(buffercache,node);
        if (rc) {  return rc; }
        rc = lhs.Serialize(buffercache,lhs_ptr);
        if (rc) {  return rc; }
        rc = rhs.Serialize(buffercache,rhs_ptr);
        return rc;
      }
    case BTREE_INTERIOR_NODE:
      for (offset=0;offset<b.info.numkeys;offset++) {
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc; }
        if (testkey==key) {  return ERROR_CONFLICT;  }
        else if (key<testkey) {
          rc=b.GetPtr(offset,ptr);
          if (rc) { return rc; }
          rc = InsertAtNode(ptr,key,value,maybe_rhs_key,maybe_rhs_ptr,rhs_created);
          if (rc) { return rc; }
          if (rhs_created) { //TODO factor into function since repeated after for loop
            rhs_created = false;
            KeyPointerPair kpp = KeyPointerPair(maybe_rhs_key, maybe_rhs_ptr);
            rc = b.InsertKeyPtr(offset,kpp);
            if (rc) {  return rc; }

            int maxkeys = b.info.GetNumSlotsAsInterior() * 2/3;
            bool TooFull = (b.info.numkeys >= maxkeys);
            if (TooFull) {
              rhs_created = true;
              rc = SplitNode(b,maybe_rhs_key,maybe_rhs_ptr);
              if (rc) {  return rc; }
            }
            rc = b.Serialize(buffercache,node);
          }
          return rc;
        }
      }
      // if we got here, we need to go to the next pointer, if it exists
      if (b.info.numkeys>0) { 
        rc=b.GetPtr(b.info.numkeys,ptr);
        if (rc) { return rc; }
        rc = InsertAtNode(ptr,key,value,maybe_rhs_key,maybe_rhs_ptr,rhs_created);
        if (rhs_created) {
          rhs_created = false;
          KeyPointerPair kpp = KeyPointerPair(maybe_rhs_key, maybe_rhs_ptr);
          rc = b.InsertKeyPtr(offset,kpp);
          if (rc) {  return rc; }

          int maxkeys = b.info.GetNumSlotsAsInterior() * 2/3;
          bool TooFull = (b.info.numkeys >= maxkeys);
          if (TooFull) {
            rhs_created = true;
            rc = SplitNode(b,maybe_rhs_key,maybe_rhs_ptr);
            if (rc) {  return rc; }
          }
          rc = b.Serialize(buffercache,node);
        }
        return rc;
      } else {
        // There are no keys at all on this node, so nowhere to go
        return ERROR_NONEXISTENT;
      }
      break;
    case BTREE_LEAF_NODE:
      for (offset=0;offset<b.info.numkeys;offset++) {
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc;  }
        if (testkey==key) {  return ERROR_CONFLICT;  }
        else if (key<testkey) {
          KeyValuePair kvp = KeyValuePair(key, value);
          rc = b.InsertKeyVal(offset,kvp);
          if (rc) {  return rc; }

          int maxkeys = b.info.GetNumSlotsAsLeaf() * 2/3;
          bool TooFull = (b.info.numkeys >= maxkeys);
          if (TooFull) {
            rhs_created = true;
            rc = SplitLeaf(b,maybe_rhs_key,maybe_rhs_ptr);
            if (rc) {  return rc; }
          }
       	  return b.Serialize(buffercache,node);
        }
      }
      if (b.info.numkeys>0) {
        KeyValuePair kvp = KeyValuePair(key, value);
        rc = b.InsertKeyVal(b.info.numkeys,kvp);
        if (rc) {  return rc; }

        int maxkeys = b.info.GetNumSlotsAsLeaf() * 2/3;
        bool TooFull = (b.info.numkeys >= maxkeys);
        if (TooFull) {
          rhs_created = true;
          rc = SplitLeaf(b,maybe_rhs_key,maybe_rhs_ptr);
          if (rc) {  return rc; }
        }
       	return b.Serialize(buffercache,node);
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




