#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include<cstdio>

// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum)
{
	this->blockNum = blockNum;
}
BlockBuffer::BlockBuffer(char blockType)
{
	// allocate a block on the disk and a buffer in memory to hold the new block of
	// given type using getFreeBlock function and get the return error codes if any.
	int type = blockType == 'R' ? REC : blockType == 'I' ? IND_INTERNAL :
					blockType == 'L' ? IND_LEAF : UNUSED_BLK; 
	
	int ret=getFreeBlock(type);
	if (ret<0 || ret>=DISK_SIZE)
	{
		this->blockNum = blockNum;
		return;
	}
	
	this->blockNum=ret;
	// set the blockNum field of the object to that of the allocated block
	// number if the method returned a valid block number,
	// otherwise set the error code returned as the block number.

	// (The caller must check if the constructor allocatted block successfully
	// by checking the value of block number field.)
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer('R'){}
// call parent non-default constructor with 'R' denoting record block.


IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType){}
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum){}

IndInternal::IndInternal() : IndBuffer('I'){}
// call the corresponding parent constructor
// 'I' used to denote IndInternal.
IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum){}

IndLeaf::IndLeaf() : IndBuffer('L'){}
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum){}


// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head)
{
	  unsigned char *bufferPtr;
	  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	  if(ret!=SUCCESS)
	  	return ret;

	  // read the block at this.blockNum into the buffer:
	  
	  // populate the numEntries, numAttrs and numSlots fields in *head
	  memcpy(&head->numSlots, bufferPtr + 24, 4);
	  memcpy(&head->numEntries, bufferPtr + 16, 4);
	  memcpy(&head->numAttrs, bufferPtr + 20, 4);
	  memcpy(&head->rblock, bufferPtr + 12, 4);
	  memcpy(&head->lblock, bufferPtr + 8, 4);
	  memcpy(&head->pblock, bufferPtr + 4, 4);

	  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{
	  struct HeadInfo head;

	  // get the header using this.getHeader() function:
	  this->getHeader(&head);

	  int attrCount = head.numAttrs;
	  int slotCount = head.numSlots;

	  // read the block at this.blockNum into a buffer:
	  unsigned char* bufferPtr;
	  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  	  if (ret != SUCCESS)
   	  	return ret;


	  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
	     - each record will have size attrCount * ATTR_SIZE
	     - slotMap will be of size slotCount
	  */
	  int recordSize = attrCount * ATTR_SIZE;
	  int slotMapSize=slotCount;
	  unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotMapSize + (recordSize * slotNum);

	  // load the record into the rec data structure
	  memcpy(rec, slotPointer, recordSize);

	  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{
	// check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
	int bufferNum=StaticBuffer::getBufferNum(this->blockNum);
	if (bufferNum!=E_BLOCKNOTINBUFFER)
	{
		for (int i=0 ; i<BUFFER_CAPACITY ; ++i)
		{
			if (StaticBuffer::metainfo[i].free==false)
				StaticBuffer::metainfo[i].timeStamp+=1;
		}
		StaticBuffer::metainfo[bufferNum].timeStamp=0;
	}

	else
	{
		bufferNum=StaticBuffer::getFreeBuffer(this->blockNum);

		if (bufferNum==E_OUTOFBOUND)
			return E_OUTOFBOUND;

		Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
	}
	// store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
	*buffPtr = StaticBuffer::blocks[bufferNum];

	return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap)
{
	unsigned char *bufferPtr;

	// get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
	int ret = loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret != SUCCESS)
		return ret;
	struct HeadInfo head;
	// get the header of the block using getHeader() function
	getHeader(&head);
	int slotCount =head.numSlots; /* number of slots in block from header */;

	// get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
	unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

	// copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
	memcpy(slotMap,slotMapInBuffer,slotCount);

	return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{

	double diff;
	// if attrType == STRING
	if (attrType==1)
		diff = strcmp(attr1.sVal, attr2.sVal);
	else
		diff = attr1.nVal - attr2.nVal;

		
	if (diff > 0)
		return 1;
	if (diff < 0)
		return -1;
	else
		return 0;
    
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int x=loadBlockAndGetBufferPtr(&bufferPtr);
	// if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
	// return the value returned by the call.
	if (x!=SUCCESS)
		return x;
	
	/* get the header of the block using the getHeader() function */
	HeadInfo head;
	this->getHeader(&head);
	// get number of attributes in the block.
	
	// get the number of slots in the block.

	// if input slotNum is not in the permitted range return E_OUTOFBOUND.
	if (slotNum<0 || slotNum>=head.numSlots)
		return E_OUTOFBOUND;

	/* offset bufferPtr to point to the beginning of the record at required
	slot. the block contains the header, the slotmap, followed by all
	the records. so, for example,
	record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
	copy the record from `rec` to buffer using memcpy
	(hint: a record will be of size ATTR_SIZE * numAttrs)
	*/
	int attrCount = head.numAttrs;
	int slotCount = head.numSlots;
	int recordSize = attrCount * ATTR_SIZE;
	int slotMapSize=slotCount;
	unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotMapSize + (recordSize * slotNum);
	
	memcpy(slotPointer, rec, recordSize);
	// update dirty bit using setDirtyBit()
	StaticBuffer::setDirtyBit(this->blockNum);

	/* (the above function call should not fail since the block is already
	in buffer and the blockNum is valid. If the call does fail, there
	exists some other issue in the code) */

	return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head)
{
	unsigned char *bufferPtr;
	// get the starting address of the buffer containing the block using
	// loadBlockAndGetBufferPtr(&bufferPtr).
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	// if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
	// return the value returned by the call.
	if (ret!=SUCCESS)
		return ret;
		
	// cast bufferPtr to type HeadInfo*
	struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

	// copy the fields of the HeadInfo pointed to by head (except reserved) to
	// the header of the block (pointed to by bufferHeader)
	//(hint: bufferHeader->numSlots = head->numSlots )
	bufferHeader->numSlots=head->numSlots;
	bufferHeader->lblock = head->lblock;
	bufferHeader->numEntries = head->numEntries;
	bufferHeader->pblock = head->pblock;
	bufferHeader->rblock = head->rblock;
	bufferHeader->blockType = head->blockType;
	bufferHeader->numAttrs=head->numAttrs;

	// update dirty bit by calling StaticBuffer::setDirtyBit()
	// if setDirtyBit() failed, return the error code
	ret=StaticBuffer::setDirtyBit(this->blockNum);
	// return SUCCESS;
	return ret;
}

int BlockBuffer::setBlockType(int blockType)
{

	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if (ret!=SUCCESS)
		return ret;

	// store the input block type in the first 4 bytes of the buffer.
	// (hint: cast bufferPtr to int32_t* and then assign it)
	// *((int32_t *)bufferPtr) = blockType;
	*((int32_t *)bufferPtr) = blockType;

	// update the StaticBuffer::blockAllocMap entry corresponding to the
	// object's block number to `blockType`.
	StaticBuffer::blockAllocMap[this->blockNum]=blockType;

	// update dirty bit by calling StaticBuffer::setDirtyBit()
	// if setDirtyBit() failed
	// return the returned value from the call
	return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType)
{

	// iterate through the StaticBuffer::blockAllocMap and find the block number
	// of a free block in the disk.
	int i;
	for(i=0; i<DISK_BLOCKS; ++i)
	{
		if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK)
			break;
	}	

	// if no block is free, return E_DISKFULL.
	if(i==DISK_BLOCKS)
		return E_DISKFULL;

	// set the object's blockNum to the block number of the free block.
	this->blockNum=i;
	
	// find a free buffer using StaticBuffer::getFreeBuffer().
	int freebuff=StaticBuffer::getFreeBuffer(i);
	struct HeadInfo head;
	head.pblock=-1;
	head.lblock=-1;
	head.rblock=-1;
	head.numEntries=0;
	head.numAttrs=0;
	head.numSlots=0;
	setHeader(&head);

	setBlockType(blockType);
	return i;
}

int RecBuffer::setSlotMap(unsigned char *slotMap)
{
	unsigned char *bufferPtr;
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if(ret!=SUCCESS)
		return ret;
		
	struct HeadInfo head;
	getHeader(&head);
	int numSlots = head.numSlots;
	memcpy(bufferPtr+HEADER_SIZE, slotMap, numSlots);
	ret= StaticBuffer::setDirtyBit(this->blockNum);
	return SUCCESS;
}

int BlockBuffer::getBlockNum()
{
	return this->blockNum;
}


void BlockBuffer::releaseBlock()
{
	if (blockNum == INVALID_BLOCKNUM || StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
		return;

	int bufferNum = StaticBuffer::getBufferNum(blockNum);
	if (bufferNum >= 0 && bufferNum < BUFFER_CAPACITY)
		StaticBuffer::metainfo[bufferNum].free = true;
	StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;
	this->blockNum = INVALID_BLOCKNUM;
}

int IndInternal::getEntry(void *ptr, int indexNum)
{
	if(indexNum<0 || indexNum>=MAX_KEYS_INTERNAL)
		return E_OUTOFBOUND;

	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);

	if(ret!=SUCCESS)
		return ret;

	// typecast the void pointer to an internal entry pointer
	struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

	/*
	- copy the entries from the indexNum`th entry to *internalEntry
	- make sure that each field is copied individually as in the following code
	- the lChild and rChild fields of InternalEntry are of type int32_t
	- int32_t is a type of int that is guaranteed to be 4 bytes across every
	C++ implementation. sizeof(int32_t) = 4
	*/

	/* the indexNum'th entry will begin at an offset of
	HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
	from bufferPtr */
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

	memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
	memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
	memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

	return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum)
{
	if(indexNum<0 || indexNum>=MAX_KEYS_LEAF)
		return E_OUTOFBOUND;
		
	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if(ret!=SUCCESS)
		return ret;

	// copy the indexNum'th Index entry in buffer to memory ptr using memcpy

	/* the indexNum'th entry will begin at an offset of
	HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
	memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

	return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum)
{

	// if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
	//     return E_OUTOFBOUND.
	if(indexNum<0 || indexNum>=MAX_KEYS_LEAF)
		return E_OUTOFBOUND;

	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if(ret!=SUCCESS)
		return ret;
		
	// copy the Index at ptr to indexNum'th entry in the buffer using memcpy

	/* the indexNum'th entry will begin at an offset of
	HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
	memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

	// update dirty bit using setDirtyBit()
	// if setDirtyBit failed, return the value returned by the call
	ret= StaticBuffer::setDirtyBit(this->blockNum);
	return ret;
}

int IndInternal::setEntry(void *ptr, int indexNum)
{
	// if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
	//     return E_OUTOFBOUND.
	if(indexNum<0 || indexNum>=MAX_KEYS_INTERNAL)
		return E_OUTOFBOUND;

	unsigned char *bufferPtr;
	/* get the starting address of the buffer containing the block
	using loadBlockAndGetBufferPtr(&bufferPtr). */
	int ret=loadBlockAndGetBufferPtr(&bufferPtr);
	if(ret!=SUCCESS)
		return ret;

	// typecast the void pointer to an internal entry pointer
	struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

	/*
	- copy the entries from *internalEntry to the indexNum`th entry
	- make sure that each field is copied individually as in the following code
	- the lChild and rChild fields of InternalEntry are of type int32_t
	- int32_t is a type of int that is guaranteed to be 4 bytes across every
	C++ implementation. sizeof(int32_t) = 4
	*/

	/* the indexNum'th entry will begin at an offset of
	HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
	from bufferPtr */

	unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

	memcpy(entryPtr, &(internalEntry->lChild), 4);
	memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
	memcpy(entryPtr + 20, &(internalEntry->rChild), 4);


	// update dirty bit using setDirtyBit()
	// if setDirtyBit failed, return the value returned by the call
	ret= StaticBuffer::setDirtyBit(this->blockNum);
	return ret;
}
