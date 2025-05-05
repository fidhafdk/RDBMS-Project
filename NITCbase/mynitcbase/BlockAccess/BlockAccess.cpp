#include "BlockAccess.h"
#include <cstring>


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















