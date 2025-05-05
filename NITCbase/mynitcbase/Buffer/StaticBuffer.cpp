#include "StaticBuffer.h"

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer()
{

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

