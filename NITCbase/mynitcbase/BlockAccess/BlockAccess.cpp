#include "BlockAccess.h"
#include <cstring>
#include<cstdlib>
#include<cstdio>


RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
	// get the previous search index of the relation relId from the relation cache
	// (use RelCacheTable::getSearchIndex() function)
	RecId prevRecId;
	RelCacheTable::getSearchIndex(relId, &prevRecId);

	// let block and slot denote the record id of the record being currently checked
	int slot,block;
	// if the current search index record is invalid(i.e. both block and slot = -1)
	if (prevRecId.block == -1 && prevRecId.slot == -1)
	{
		// (no hits from previous search; search should start from the
		// first record itself)

		// get the first record block of the relation from the relation cache
		// (use RelCacheTable::getRelCatEntry() function of Cache Layer)
		RelCatEntry firstrecblock;
		RelCacheTable::getRelCatEntry(relId, &firstrecblock);

		block = firstrecblock.firstBlk;
		slot = 0;
	}
	else
	{
		// (there is a hit from previous search; search should start from
		// the record next to the search index record)

		block = prevRecId.block;
		slot = prevRecId.slot + 1;
	}

	/* The following code searches for the next record in the relation
	that satisfies the given condition
	We start from the record id (block, slot) and iterate over the remaining
	records of the relation
	*/
	while (block != -1)
	{
		/* create a RecBuffer object for block (use RecBuffer Constructor for
		existing block) */
		RecBuffer recbuffer(block);
		
		// get header of the block using RecBuffer::getHeader() function
		struct HeadInfo head;
		recbuffer.getHeader(&head);
		// get slot map of the block using RecBuffer::getSlotMap() function		
		unsigned char slotmap[head.numSlots];
		recbuffer.getSlotMap(slotmap);
		
		// get the record with id (block, slot) using RecBuffer::getRecord()
		union Attribute record[head.numAttrs];
		recbuffer.getRecord(record, slot);
		

		if ( slot >= head.numSlots)
		{
			block = head.rblock;
			slot = 0;
			continue;  // continue to the beginning of this while loop
		}

		// if slot is free skip the loop
		// (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
		if (slotmap[slot]==SLOT_UNOCCUPIED)
		{
			slot++;
			continue;
		}

		// compare record's attribute value to the the given attrVal as below:
		/*
		firstly get the attribute offset for the attrName attribute
		from the attribute cache entry of the relation using
		AttrCacheTable::getAttrCatEntry()
		*/
		AttrCatEntry attrcatentry;
		AttrCacheTable::getAttrCatEntry(relId, attrName,&attrcatentry);
		/* use the attribute offset to get the value of the attribute from
		current record */

		int cmpVal;  // will store the difference between the attributes
		// set cmpVal using compareAttrs()
		cmpVal=compareAttrs(record[attrcatentry.offset], attrVal, attrcatentry.attrType);

		/* Next task is to check whether this record satisfies the given condition.
		It is determined based on the output of previous comparison and
		the op value received.
		The following code sets the cond variable if the condition is satisfied.
		*/
		if (
		(op == NE && cmpVal != 0) ||    // if op is "not equal to"
		(op == LT && cmpVal < 0) ||     // if op is "less than"
		(op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
		(op == EQ && cmpVal == 0) ||    // if op is "equal to"
		(op == GT && cmpVal > 0) ||     // if op is "greater than"
		(op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
		)
		{
			/*
			set the search index in the relation cache as
			the record id of the record that satisfies the given condition
			(use RelCacheTable::setSearchIndex function)
			*/
			
			RecId recid{block,slot};
			RelCacheTable::setSearchIndex(relId,&recid);
			return RecId{block, slot};
		}

		slot++;
	}

	// no record in the relation with Id relid satisfies the given condition
	return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
	/* reset the searchIndex of the relation catalog using
	RelCacheTable::resetSearchIndex() */
	
	RelCacheTable::resetSearchIndex(RELCAT_RELID);
	
	Attribute newRelationName;    // set newRelationName with newName
	strcpy(newRelationName.sVal,newName);

	// search the relation catalog for an entry with "RelName" = newRelationName
	char RelName[ATTR_SIZE];
	strcpy(RelName,"RelName");
	RecId searchResult = linearSearch(RELCAT_RELID, RelName, newRelationName, EQ);
	
	// If relation with name newName already exists (result of linearSearch
	//                                               is not {-1, -1})
	//    return E_RELEXIST;
	if (searchResult.block!=-1 && searchResult.slot!=-1)
		return E_RELEXIST;

	/* reset the searchIndex of the relation catalog using
	RelCacheTable::resetSearchIndex() */
	RelCacheTable::resetSearchIndex(RELCAT_RELID);
	
	Attribute oldRelationName;    // set oldRelationName with oldName
	strcpy(oldRelationName.sVal, oldName);  // Set oldRelationName with oldName
	searchResult = linearSearch(RELCAT_RELID, RelName, oldRelationName, EQ);

	// search the relation catalog for an entry with "RelName" = oldRelationName

	// If relation with name oldName does not exist (result of linearSearch is {-1, -1})
	//    return E_RELNOTEXIST;
	
	if (searchResult.block==-1 && searchResult.slot==-1)
		return E_RELNOTEXIST;


	/* get the relation catalog record of the relation to rename using a RecBuffer
	on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
	*/
	RecBuffer recbuffer(searchResult.block);
	Attribute attribute[RELCAT_NO_ATTRS];
	recbuffer.getRecord(attribute, searchResult.slot);
	
	/* update the relation name attribute in the record with newName.
	(use RELCAT_REL_NAME_INDEX) */
	strcpy(attribute[RELCAT_REL_NAME_INDEX].sVal,newName);
	recbuffer.setRecord(attribute, searchResult.slot);
	// set back the record value using RecBuffer.setRecord
	
	

	/*
	update all the attribute catalog entries in the attribute catalog corresponding
	to the relation with relation name oldName to the relation name newName
	*/

	/* reset the searchIndex of the attribute catalog using
	RelCacheTable::resetSearchIndex() */
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

	while(true)
	{
		RecId attrSearchResult=linearSearch(ATTRCAT_RELID, RelName,oldRelationName,EQ);
		if (attrSearchResult.block==-1 && attrSearchResult.slot==-1)
			break;

		RecBuffer attrrecbuffer(attrSearchResult.block);
		Attribute attrib[ATTRCAT_NO_ATTRS];
		attrrecbuffer.getRecord(attrib,attrSearchResult.slot);

		strcpy(attrib[ATTRCAT_REL_NAME_INDEX].sVal,newName);
		attrrecbuffer.setRecord(attrib, attrSearchResult.slot);		
	}
	//for i = 0 to numberOfAttributes :
	//    linearSearch on the attribute catalog for relName = oldRelationName
	//    get the record using RecBuffer.getRecord
	//
	//    update the relName field in the record to newName
	//    set back the record using RecBuffer.setRecord

	return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

	/* reset the searchIndex of the relation catalog using
	RelCacheTable::resetSearchIndex() */
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

	Attribute relNameAttr;    // set relNameAttr to relName
	strcpy(relNameAttr.sVal,relName);
	
	// Search for the relation with name relName in relation catalog using linearSearch()
	// If relation with name relName does not exist (search returns {-1,-1})
	//    return E_RELNOTEXIST;
	char RelName[ATTR_SIZE];
	strcpy(RelName,"RelName");
	RecId relsearch=linearSearch(RELCAT_RELID,RelName,relNameAttr,EQ);
	if (relsearch.block==-1 && relsearch.slot==-1)
		return E_RELNOTEXIST;

	/* reset the searchIndex of the attribute catalog using
	RelCacheTable::resetSearchIndex() */
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

	/* declare variable attrToRenameRecId used to store the attr-cat recId
	of the attribute to rename */
	RecId attrToRenameRecId;
	Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

	/* iterate over all Attribute Catalog Entry record corresponding to the
	relation to find the required attribute */
	while (true)
	{
		// linear search on the attribute catalog for RelName = relNameAttr
		attrToRenameRecId=linearSearch(ATTRCAT_RELID,RelName,relNameAttr,EQ);
		
		// if there are no more attributes left to check (linearSearch returned {-1,-1})
		//     break;
		if (attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1)
			break;

		/* Get the record from the attribute catalog using RecBuffer.getRecord
		into attrCatEntryRecord */
		
		RecBuffer recbuffer(attrToRenameRecId.block);
		recbuffer.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);

		// if attrCatEntryRecord.attrName = oldName
		//     attrToRenameRecId = block and slot of this record
		if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName)==0)
			return E_ATTREXIST;
			
		if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldName)==0)
		{
			RecBuffer attrcatrecbuffer(attrToRenameRecId.block);
			attrcatrecbuffer.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);
			strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);
			attrcatrecbuffer.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);
			break;
		}

		// if attrCatEntryRecord.attrName = newName
		//     return E_ATTREXIST;
	}

	if (attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1)
		return E_ATTRNOTEXIST;

	// Update the entry corresponding to the attribute in the Attribute Catalog Relation.
	/*   declare a RecBuffer for attrToRenameRecId.block and get the record at
	attrToRenameRecId.slot */
	//   update the AttrName of the record with newName
	//   set back the record with RecBuffer.setRecord
	
	
	

	return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
	RelCatEntry relcatentry;
	RelCacheTable::getRelCatEntry(relId,&relcatentry);

	int blockNum = relcatentry.firstBlk;

	// rec_id will be used to store where the new record will be inserted
	RecId rec_id = {-1, -1};

	int numOfSlots = relcatentry.numSlotsPerBlk;
	int numOfAttributes = relcatentry.numAttrs;

	int prevBlockNum =-1;

	/*
	Traversing the linked list of existing record blocks of the relation
	until a free slot is found OR
	until the end of the list is reached
	*/
	while (blockNum != -1)
	{
		// create a RecBuffer object for blockNum (using appropriate constructor!)
		RecBuffer recbuffer(blockNum);
		// get header of block(blockNum) using RecBuffer::getHeader() function
		struct HeadInfo Head;
		recbuffer.getHeader(&Head);
		// get slot map of block(blockNum) using RecBuffer::getSlotMap() function
		unsigned char *slotMap=(unsigned char *)malloc(sizeof(unsigned char)*numOfSlots);
		recbuffer.getSlotMap(slotMap); 

		// search for free slot in the block 'blockNum' and store it's rec-id in rec_id
		// (Free slot can be found by iterating over the slot map of the block)
		/* slot map stores SLOT_UNOCCUPIED if slot is free and
		SLOT_OCCUPIED if slot is occupied) */
		int slot;
		for(slot=0;slot<numOfSlots;++slot)
		{
			if(slotMap[slot]==SLOT_UNOCCUPIED)
			{
				rec_id.block=blockNum;
				rec_id.slot=slot;
				break;
			}
		}
		if(rec_id.block!=-1 && rec_id.slot!=-1)
		{
			break;
		}

		/* otherwise, continue to check the next block by updating the
		block numbers as follows:
		update prevBlockNum = blockNum
		update blockNum = header.rblock (next element in the linked list of record blocks)
		*/
		prevBlockNum = blockNum;
		blockNum = Head.rblock;
	}

	if(rec_id.block==-1 && rec_id.slot==-1)
	{
		// if relation is RELCAT, do not allocate any more blocks
		//     return E_MAXRELATIONS;
		if(relId==RELCAT_RELID)
			return E_MAXRELATIONS;

		// Otherwise,
		// get a new record block (using the appropriate RecBuffer constructor!)
		RecBuffer recbuff;
		int ret=recbuff.getBlockNum();
		// get the block number of the newly allocated block
		// (use BlockBuffer::getBlockNum() function)
		
		// let ret be the return value of getBlockNum() function call
		if (ret == E_DISKFULL)
			return E_DISKFULL;

		// Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
		rec_id.block=ret;
		rec_id.slot=0;

		struct HeadInfo header;
		header.blockType=REC;
		header.pblock=-1;
		header.lblock=prevBlockNum;
		header.rblock=-1;
		header.numEntries=0;
		header.numSlots=relcatentry.numSlotsPerBlk;
		header.numAttrs=relcatentry.numAttrs;
		RecBuffer newblk(ret);
		newblk.setHeader(&header);

		/*
		set block's slot map with all slots marked as free
		(i.e. store SLOT_UNOCCUPIED for all the entries)
		(use RecBuffer::setSlotMap() function)
		*/
		unsigned char *newblkslotMap= (unsigned char *)malloc(sizeof(unsigned char)*relcatentry.numSlotsPerBlk);
		for(int i=0;i<relcatentry.numSlotsPerBlk;++i)
			newblkslotMap[i]=SLOT_UNOCCUPIED;
		newblk.setSlotMap(newblkslotMap);

	
		if(prevBlockNum!=-1)
		{
			RecBuffer prevrecbuff(prevBlockNum);
			struct HeadInfo prevhead;
			prevrecbuff.getHeader(&prevhead);
			prevhead.rblock=rec_id.block;
			// (use BlockBuffer::setHeader() function)
			prevrecbuff.setHeader(&prevhead);
		}
		else
		{
			// update first block field in the relation catalog entry to the
			// new block (using RelCacheTable::setRelCatEntry() function)
			relcatentry.firstBlk=rec_id.block;
		}

		// update last block field in the relation catalog entry to the
		// new block (using RelCacheTable::setRelCatEntry() function)
		relcatentry.lastBlk=rec_id.block;
		RelCacheTable::setRelCatEntry(relId, &relcatentry);
		
	}

	// create a RecBuffer object for rec_id.block
	// insert the record into rec_id'th slot using RecBuffer.setRecord())
	RecBuffer newblk(rec_id.block);
	newblk.setRecord(record, rec_id.slot);
	
	unsigned char *newslotmap=(unsigned char *)malloc(sizeof(unsigned char )*numOfSlots);
	newblk.getSlotMap(newslotmap);
	newslotmap[rec_id.slot]=SLOT_OCCUPIED;
	newblk.setSlotMap(newslotmap);
	// increment the numEntries field in the header of the block to
	// which record was inserted
	// (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
	struct HeadInfo newheader;
	newblk.getHeader(&newheader);
	(newheader.numEntries)=(newheader.numEntries)+1;
	newblk.setHeader(&newheader);
	// Increment the number of records field in the relation cache entry for
	// the relation. (use RelCacheTable::setRelCatEntry function)
	relcatentry.numRecs=(relcatentry.numRecs)+1;
	RelCacheTable::setRelCatEntry(relId, &relcatentry);
	
	/* B+ Tree Insertions */
	// (the following section is only relevant once indexing has been implemented)

	int flag = SUCCESS;
	// Iterate over all the attributes of the relation
	for(int attrOffset=0 ; attrOffset<numOfAttributes ; attrOffset++)
	{
		// get the attribute catalog entry for the attribute from the attribute cache
		// (use AttrCacheTable::getAttrCatEntry() with args relId and attrOffset)
		AttrCatEntry attrCatEntry;
		AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatEntry);

		// get the root block field from the attribute catalog entry
		int rootblk=attrCatEntry.rootBlock;
		if(rootblk!=-1)
		{
			/* insert the new record into the attribute's bplus tree using
			BPlusTree::bPlusInsert()*/
			int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName, record[attrOffset], rec_id);

			if (retVal == E_DISKFULL)
			{
				//(index for this attribute has been destroyed)
				flag = E_INDEX_BLOCKS_RELEASED;
			}
		}
	}

	return flag;

}

/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
	// Declare a variable called recid to store the searched record
	RecId recId;

	/* get the attribute catalog entry from the attribute cache corresponding
	to the relation with Id=relid and with attribute_name=attrName  */
	AttrCatEntry attrCatBuf;
	int ret=AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
	if(ret!=SUCCESS)
		return ret;

	// get rootBlock from the attribute catalog entry
	if(attrCatBuf.rootBlock==-1)
	{
		
		recId=BlockAccess::linearSearch(relId, attrName, attrVal, op);
	}

	else
	{	
		recId=BPlusTree::bPlusSearch(relId, attrName,  attrVal, op);
	}


	// if there's no record satisfying the given condition (recId = {-1, -1})
	//     return E_NOTFOUND;
	if(recId.block==-1 || recId.slot==-1)
		return E_NOTFOUND;

	RecBuffer recbuffer(recId.block);
	recbuffer.getRecord(record, recId.slot);
//	if (record->nVal==0)
//		printf("%d %d\n",recId.block,recId.slot);
	return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{
	if(strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
		return E_NOTPERMITTED;

	/* reset the searchIndex of the relation catalog using
	RelCacheTable::resetSearchIndex() */
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

	Attribute relNameAttr; // (stores relName as type union Attribute)
	strcpy(relNameAttr.sVal, relName);
	//  linearSearch on the relation catalog for RelName = relNameAttr
	char RelName[8];
	strcpy( RelName,"RelName");
	RecId relid=linearSearch(RELCAT_RELID,RelName , relNameAttr, EQ);

	if(relid.block==-1 && relid.slot==-1)
		return E_RELNOTEXIST;

	Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
	/* store the relation catalog record corresponding to the relation in
	relCatEntryRecord using RecBuffer.getRecord */
	RecBuffer recbuffer(relid.block);
	recbuffer.getRecord(relCatEntryRecord,relid.slot);

	/* get the first record block of the relation (firstBlock) using the
	relation catalog entry record */
	/* get the number of attributes corresponding to the relation (numAttrs)
	using the relation catalog entry record */
	int firstblock=relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
	int noofattrs=relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
	

	/*Delete all the record blocks of the relation */
	// for each record block of the relation:
	//     get block header using BlockBuffer.getHeader
	//     get the next block from the header (rblock)
	//     release the block using BlockBuffer.releaseBlock
	//
	//     Hint: to know if we reached the end, check if nextBlock = -1
	int currblock=firstblock;
	while (currblock!=-1)
	{
		RecBuffer recbuff(currblock);
		HeadInfo head;
		recbuff.getHeader(&head);
		int next=head.rblock;
		recbuff.releaseBlock();
		currblock=next;
	}
	/***
	Deleting attribute catalog entries corresponding the relation and index
	blocks corresponding to the relation with relName on its attributes
	***/

	// reset the searchIndex of the attribute catalog
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

	int numberOfAttributesDeleted = 0;

 	while(true)
 	{
		RecId attrCatRecId;
		char AttrRelName[8];
		strcpy(AttrRelName,ATTRCAT_ATTR_RELNAME);
		attrCatRecId = linearSearch(ATTRCAT_RELID, AttrRelName, relNameAttr, EQ);
		if(attrCatRecId.block==-1 && attrCatRecId.slot==-1)
			break;

		numberOfAttributesDeleted++;

		// create a RecBuffer for attrCatRecId.block
		RecBuffer recbuff(attrCatRecId.block);
		// get the header of the block
		HeadInfo head;
		recbuff.getHeader(&head);
		// get the record corresponding to attrCatRecId.slot
		union Attribute attribute[head.numAttrs];
		recbuff.getRecord(attribute,attrCatRecId.slot);

		// declare variable rootBlock which will be used to store the root
		// block field from the attribute catalog record.
		int rootBlock = attribute[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
		// (This will be used later to delete any indexes if it exists)

		// Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
		// Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
		unsigned char* slotmap=(unsigned char*)malloc(sizeof(unsigned char)*head.numSlots);
		recbuff.getSlotMap(slotmap);
		slotmap[attrCatRecId.slot]=SLOT_UNOCCUPIED;
		recbuff.setSlotMap(slotmap);
		
		/* Decrement the numEntries in the header of the block corresponding to
		the attribute catalog entry and then set back the header
		using RecBuffer.setHeader */
		head.numEntries=head.numEntries-1;
		recbuff.setHeader(&head);

		/* If number of entries become 0, releaseBlock is called after fixing
		the linked list.
		*/
		if (head.numEntries==0)
		{
			/* Standard Linked List Delete for a Block
			Get the header of the left block and set it's rblock to this
			block's rblock
			*/
			// create a RecBuffer for lblock and call appropriate methods
			HeadInfo lefthead;
			RecBuffer leftbuff(head.lblock);
			leftbuff.getHeader(&lefthead);
			lefthead.rblock=head.rblock;
			leftbuff.setHeader(&lefthead);

			if (head.rblock!=-1)
			{
				/* Get the header of the right block and set it's lblock to
				this block's lblock */
				// create a RecBuffer for rblock and call appropriate methods
				HeadInfo righthead;
				RecBuffer rightbuff(head.rblock);
				rightbuff.getHeader(&righthead);
				righthead.lblock=head.lblock;
				rightbuff.setHeader(&righthead);
			}
			else
			{
				// (the block being released is the "Last Block" of the relation.)
				/* update the Relation Catalog entry's LastBlock field for this
				relation with the block number of the previous block. */
				RelCatEntry relCatEntryBuffer;
				RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
				relCatEntryBuffer.lastBlk = head.lblock;
			}

			// (Since the attribute catalog will never be empty(why?), we do not
			//  need to handle the case of the linked list becoming empty - i.e
			//  every block of the attribute catalog gets released.)

			recbuff.releaseBlock();
		}

// (the following part is only relevant once indexing has been implemented)
		// if index exists for the attribute (rootBlock != -1), call bplus destroy
		if (rootBlock != -1)
		{
			// delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
			BPlusTree::bPlusDestroy(rootBlock);
		}
	}

	/*** Delete the entry corresponding to the relation from relation catalog ***/
	// Fetch the header of Relcat block
	RecBuffer relcatbuffer(relid.block);
	HeadInfo relcatheader;
	relcatbuffer.getHeader(&relcatheader);
	/* Decrement the numEntries in the header of the block corresponding to the
	relation catalog entry and set it back */
	relcatheader.numEntries=relcatheader.numEntries-1;
	relcatbuffer.setHeader(&relcatheader);

	/* Get the slotmap in relation catalog, update it by marking the slot as
	free(SLOT_UNOCCUPIED) and set it back. */
	unsigned char* slotMap=(unsigned char*)malloc(sizeof(unsigned char)*relcatheader.numSlots);
	relcatbuffer.getSlotMap(slotMap);
	slotMap[relid.slot]=SLOT_UNOCCUPIED;
	relcatbuffer.setSlotMap(slotMap);	

	/*** Updating the Relation Cache Table ***/
	/** Update relation catalog record entry (number of records in relation
	catalog is decreased by 1) **/
	// Get the entry corresponding to relation catalog from the relation
	// cache and update the number of records and set it back
	// (using RelCacheTable::setRelCatEntry() function)
	RelCatEntry relCatBuf;
	RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
	relCatBuf.numRecs=relCatBuf.numRecs-1;
	RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);
	 
	/** Update attribute catalog entry (number of records in attribute catalog
	is decreased by numberOfAttributesDeleted) **/
	// i.e., #Records = #Records - numberOfAttributesDeleted
	RelCatEntry attrCatBuf;
	RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
	attrCatBuf.numRecs=attrCatBuf.numRecs- numberOfAttributesDeleted;
	RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
	
	// Get the entry corresponding to attribute catalog from the relation
	// cache and update the number of records and set it back
	// (using RelCacheTable::setRelCatEntry() function)

	return SUCCESS;
}

/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/

//to fetch one record of the relation in the next serach index without any conditions

int BlockAccess::project(int relId, Attribute *record)
{
	// get the previous search index of the relation relId from the relation
	// cache (use RelCacheTable::getSearchIndex() function)
	RecId prevRecId;
	RelCacheTable::getSearchIndex(relId, &prevRecId);

	// declare block and slot which will be used to store the record id of the
	// slot we need to check.
	int block, slot;

	/* if the current search index record is invalid(i.e. = {-1, -1})
	(this only happens when the caller reset the search index)
	*/
	if (prevRecId.block == -1 && prevRecId.slot == -1)
	{
		// (new project operation. start from beginning)

		// get the first record block of the relation from the relation cache
		// (use RelCacheTable::getRelCatEntry() function of Cache Layer)
		RelCatEntry relCatBuf;
		RelCacheTable::getRelCatEntry(relId, &relCatBuf);
		block=relCatBuf.firstBlk;
		slot=0;

		// block = first record block of the relation
		// slot = 0
	}
	else
	{
		// (a project/search operation is already in progress)

		// block = previous search index's block
		// slot = previous search index's slot + 1
		block=prevRecId.block;
		slot=prevRecId.slot+1;
	}


	// The following code finds the next record of the relation
	/* Start from the record id (block, slot) and iterate over the remaining
	records of the relation */
	while (block != -1)
	{
		// create a RecBuffer object for block (using appropriate constructor!)
		RecBuffer recbuffer(block);
		// get header of the block using RecBuffer::getHeader() function
		// get slot map of the block using RecBuffer::getSlotMap() function
		HeadInfo head;
		recbuffer.getHeader(&head);
		unsigned char * slotmap=(unsigned char*)malloc(sizeof(unsigned char)*head.numSlots);
		recbuffer.getSlotMap(slotmap);

		if(slot>=head.numSlots)
		{
			// (no more slots in this block)
			// update block = right block of block
			// update slot = 0
			block=head.rblock;
			slot=0;
		}
		else if (slotmap[slot]==SLOT_UNOCCUPIED)
		{	
			 // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)
			// increment slot
			slot++;
		}
		else 
		{
			// (the next occupied slot / record has been found)
			break;
		}
	}

	if (block == -1)
	{
		// (a record was not found. all records exhausted)
		return E_NOTFOUND;
	}

	// declare nextRecId to store the RecId of the record found
	RecId nextRecId{block, slot};
	// set the search index to nextRecId using RelCacheTable::setSearchIndex
	RelCacheTable::setSearchIndex(relId, &nextRecId);

	/* Copy the record with record id (nextRecId) to the record buffer (record)
	For this Instantiate a RecBuffer class object by passing the recId and
	call the appropriate method to fetch the record
	*/
	RecBuffer recbuff(nextRecId.block);
	recbuff.getRecord(record, nextRecId.slot);

	return SUCCESS;
}
