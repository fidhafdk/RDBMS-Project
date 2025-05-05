#include "StaticBuffer.h"

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
// declare the blockAllocMap array
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];


StaticBuffer::StaticBuffer()
{
	// copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
	// blocks 0 to 3
	for (int i=0, blockMapslot=0; i<4; ++i)
	{	
		unsigned char buffer[BLOCK_SIZE];
		Disk::readBlock(buffer, i);
		for (int slot=0; slot<BLOCK_SIZE; slot++, blockMapslot++)
		{
			StaticBuffer::blockAllocMap[blockMapslot] = buffer[slot];
		}
	}
	/* initialise metainfo of all the buffer blocks with
	dirty:false, free:true, timestamp:-1 and blockNum:-1
	(you did this already)
	*/
	// initialise all blocks as free
	for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; ++bufferIndex)
	{
		metainfo[bufferIndex].free = true;
		metainfo[bufferIndex].dirty=false;
		metainfo[bufferIndex].timeStamp=-1;
		metainfo[bufferIndex].blockNum=-1;
	}
}

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer()
{
	// copy blockAllocMap blocks from buffer to disk(using writeblock() of disk)
	for (int i=0, blockMapslot=0; i<4; i++)
	{
		unsigned char buffer[BLOCK_SIZE];
		for (int slot=0; slot<BLOCK_SIZE; slot++, blockMapslot++)
		{
			buffer[slot] = blockAllocMap[blockMapslot];
		}
		Disk::writeBlock(buffer, i);
	}
	/*iterate through all the buffer blocks,
	write back blocks with metainfo as free:false,dirty:true
	(you did this already)
	*/
	

	for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; ++bufferIndex)
	{
		if (metainfo[bufferIndex].free==false && metainfo[bufferIndex].dirty==true)
			Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
	}
	
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
	if (blockNum < 0 || blockNum >= DISK_BLOCKS)
		return E_OUTOFBOUND;
	int bufferNum=-1;

	for (int i = 0; i < BUFFER_CAPACITY; ++i)
	{
		if(metainfo[i].free==false)
			metainfo[i].timeStamp+=1;
	}
	// iterate through all the blocks in the StaticBuffer
	// find the first free block in the buffer (check metainfo)
	// assign allocatedBuffer = index of the free block
	for (int i = 0; i < BUFFER_CAPACITY; ++i)
	{
		if(metainfo[i].free)
		{
			bufferNum=i;
			break;
		}
	}

	if (bufferNum==-1)
	{
		int max=0;
		for (int i = 1; i < BUFFER_CAPACITY; ++i)
		{
			if (metainfo[max].timeStamp<metainfo[i].timeStamp)
				max=i;
		}
		
		bufferNum=max;
		if (metainfo[bufferNum].dirty==true)
		{
			Disk::writeBlock(blocks[bufferNum], metainfo[bufferNum].blockNum);
		}
	}
		
	metainfo[bufferNum].free=false;
	metainfo[bufferNum].blockNum=blockNum;
	metainfo[bufferNum].timeStamp=0;
			

	return bufferNum;
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum)
{
	// Check if blockNum is valid (between zero and DISK_BLOCKS)
	// and return E_OUTOFBOUND if not valid.
	if(blockNum<0 || blockNum>=DISK_BLOCKS)
		return E_OUTOFBOUND;
	// find and return the bufferIndex which corresponds to blockNum (check metainfo)
	for (int i = 0; i < BUFFER_CAPACITY; ++i)
	{
		if(metainfo[i].free==false && metainfo[i].blockNum==blockNum)
		{
			return i;
		}
	}
  
	return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
	// find the buffer index corresponding to the block using getBufferNum().
	int buffindex=getBufferNum(blockNum);
	if (buffindex==E_BLOCKNOTINBUFFER)
		return E_BLOCKNOTINBUFFER;
	if (buffindex==E_OUTOFBOUND)
		return E_OUTOFBOUND;

	// else
	//     (the bufferNum is valid)
	//     set the dirty bit of that buffer to true in metainfo
	
	metainfo[buffindex].dirty=true;

	return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum)
{
	// Check if blockNum is valid (non zero and less than number of disk blocks)
	// and return E_OUTOFBOUND if not valid.
	if(blockNum<0 || blockNum>=DISK_BLOCKS)
		return E_OUTOFBOUND;

	// Access the entry in block allocation map corresponding to the blockNum argument
	// and return the block type after type casting to integer.
	return (int)blockAllocMap[blockNum];
}

