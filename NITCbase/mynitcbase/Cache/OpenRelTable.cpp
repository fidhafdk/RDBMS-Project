#include "OpenRelTable.h"
#include <cstring>
#include<cstdlib>
#include<cstdio>


OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable()
{

	// initialise all values in relCache and attrCache to be nullptr and all entries
	// in tableMetaInfo to be free
	
	for (int i = 0; i < MAX_OPEN; ++i)
	{
		RelCacheTable::relCache[i]=nullptr;
		AttrCacheTable::attrCache[i]=nullptr;
		OpenRelTable::tableMetaInfo[i].free=true;
	}
	
	

	// load the relation and attribute catalog into the relation cache (we did this already)
	
	RecBuffer relCatBlock(RELCAT_BLOCK);
	Attribute relCatRecord[RELCAT_NO_ATTRS];
	relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

	struct RelCacheEntry relCacheEntry;  
	RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
	relCacheEntry.recId.block = RELCAT_BLOCK;
	relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;//relCacheEntry has relationship catalog details


	RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
	*(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;
	
	
	RecBuffer attrCatBlock(RELCAT_BLOCK);
	Attribute attrCatRecord[ATTRCAT_NO_ATTRS];	
	attrCatBlock.getRecord(attrCatRecord,RELCAT_SLOTNUM_FOR_ATTRCAT);

	struct RelCacheEntry relCacheEntry2;
	RelCacheTable::recordToRelCatEntry(attrCatRecord, &relCacheEntry2.relCatEntry);
	relCacheEntry2.recId.block = ATTRCAT_BLOCK;
	relCacheEntry2.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
	
	RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
	*(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry2;
	
	// load the relation and attribute catalog into the attribute cache (we did this already)
	AttrCacheEntry *prevEntry=nullptr,*head=nullptr;
	
	RecBuffer  attrCatblock(ATTRCAT_BLOCK);
	for(int i=0;i<RELCAT_NO_ATTRS;++i)
	{
		attrCatblock.getRecord(attrCatRecord,i);
		
		AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
		attrCacheEntry->recId.block=ATTRCAT_BLOCK;
		attrCacheEntry->recId.slot=i;
		attrCacheEntry->next=nullptr;
		AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
		
		if(prevEntry!=nullptr)
			prevEntry->next=attrCacheEntry;
		else
			head=attrCacheEntry;
			
		prevEntry=attrCacheEntry;
	}
	AttrCacheTable::attrCache[RELCAT_RELID]=head;
	
	prevEntry=nullptr;
	head=nullptr;
	
	for(int i=RELCAT_NO_ATTRS;i<RELCAT_NO_ATTRS+ATTRCAT_NO_ATTRS;++i)
	{
		attrCatblock.getRecord(attrCatRecord,i);
		
		AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
		attrCacheEntry->recId.block=ATTRCAT_BLOCK;
		attrCacheEntry->recId.slot=i;
		attrCacheEntry->next=nullptr;
		AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);		
		
		if(prevEntry!=nullptr)
			prevEntry->next=attrCacheEntry;
		else
			head=attrCacheEntry;
			
		prevEntry=attrCacheEntry;
	}
	AttrCacheTable::attrCache[ATTRCAT_RELID]=head;
	
	/************ Setting up tableMetaInfo entries ************/
	OpenRelTable::tableMetaInfo[RELCAT_RELID].free=false;
	strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
	
	OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free=false;
	strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);
}


OpenRelTable::~OpenRelTable()
{

	// close all open relations (from rel-id = 2 onwards. Why?)
	for (int i = 2; i < MAX_OPEN; ++i)
	{
		if (!tableMetaInfo[i].free)
		{
			OpenRelTable::closeRel(i); // we will implement this function later
		}
	}

	// free the memory allocated for rel-id 0 and 1 in the caches
	
	for (int i = 0; i < 2; ++i)
	{
		if (RelCacheTable::relCache[i])
		{
			free(RelCacheTable::relCache[i]);
			RelCacheTable::relCache[i] = nullptr;
		}
	}

	// Free attribute cache entries
	for (int i = 0; i < 2; ++i)
	{
		AttrCacheEntry *current = AttrCacheTable::attrCache[i];
		while (current)
		{
			AttrCacheEntry *toDelete = current;
			current = current->next;
			free(toDelete);
		}
		AttrCacheTable::attrCache[i] = nullptr;
	}
}


int OpenRelTable::getFreeOpenRelTableEntry()
{

	/* traverse through the tableMetaInfo array,
	find a free entry in the Open Relation Table.*/
	for (int i=2;i<MAX_OPEN;++i)
	{
		if (tableMetaInfo[i].free==true)
			return i;
	}
	
	return E_CACHEFULL;

	// if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{

	/* traverse through the tableMetaInfo array,
	find the entry in the Open Relation Table corresponding to relName.*/
	
	for(int i=0;i<MAX_OPEN;++i)
	{
		if(strcmp(relName,tableMetaInfo[i].relName)==0)
			return i;
	}
	return E_RELNOTOPEN;

	// if found return the relation id, else indicate that the relation do not
	// have an entry in the Open Relation Table.
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
	int x=OpenRelTable::getRelId(relName);
	if(x!=E_RELNOTOPEN)
	{
		// (checked using OpenRelTable::getRelId())
		// return that relation id;
		return x;
	}

	/* find a free slot in the Open Relation Table
	using OpenRelTable::getFreeOpenRelTableEntry(). */
	int relId=OpenRelTable::getFreeOpenRelTableEntry();

	if (relId==E_CACHEFULL)
	{
		return E_CACHEFULL;
	}

	// let relId be used to store the free slot.
	

	/****** Setting up Relation Cache entry for the relation ******/

	/* search for the entry with relation name, relName, in the Relation Catalog using
	BlockAccess::linearSearch().
	Care should be taken to reset the searchIndex of the relation RELCAT_RELID
	before calling linearSearch().*/
	char attrName[ATTR_SIZE];
	strcpy(attrName,"RelName");
	Attribute attrVal;
	strcpy(attrVal.sVal,relName);
	RelCacheTable::resetSearchIndex(RELCAT_RELID);
	// relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
	RecId relcatRecId=BlockAccess::linearSearch(RELCAT_RELID, attrName, attrVal, EQ);

	

	if (relcatRecId.block==-1 && relcatRecId.slot==-1)
	{
		// (the relation is not found in the Relation Catalog.)
		return E_RELNOTEXIST;
	}

	/* read the record entry corresponding to relcatRecId and create a relCacheEntry
	on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
	update the recId field of this Relation Cache entry to relcatRecId.
	use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
	NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
	*/
	Attribute record[NO_OF_ATTRS_RELCAT_ATTRCAT];
	RelCacheEntry* relcacheEntry=(RelCacheEntry*)malloc(sizeof(RelCacheEntry));
	
	RecBuffer recBuffer(relcatRecId.block);
	recBuffer.getRecord(record, relcatRecId.slot);
	RelCacheTable::recordToRelCatEntry(record,&(relcacheEntry->relCatEntry));
	relcacheEntry->recId.block=relcatRecId.block;
	relcacheEntry->recId.slot=relcatRecId.slot;
	relcacheEntry->dirty=false;
	
	//RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
	RelCacheTable::relCache[relId] = relcacheEntry;
	

	/****** Setting up Attribute Cache entry for the relation ******/

	// let listHead be used to hold the head of the linked list of attrCache entries.
	AttrCacheEntry* listHead=nullptr;
	RecId attrcatRecId;
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
	/*iterate over all the entries in the Attribute Catalog corresponding to each
	attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
	care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
	corresponding to Attribute Catalog before the first call to linearSearch().*/
	while((attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,attrName,attrVal,EQ)).block!=-1)
	{
	
		/* let attrcatRecId store a valid record id an entry of the relation, relName,
		in the Attribute Catalog.*/
		

		/* read the record entry corresponding to attrcatRecId and create an
		Attribute Cache entry on it using RecBuffer::getRecord() and
		AttrCacheTable::recordToAttrCatEntry().
		update the recId field of this Attribute Cache entry to attrcatRecId.
		add the Attribute Cache entry to the linked list of listHead .*/
		// NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
		
		Attribute Record[NO_OF_ATTRS_RELCAT_ATTRCAT];
		AttrCacheEntry* attrcacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
		
		RecBuffer recbuffer(attrcatRecId.block);
		recbuffer.getRecord(Record, attrcatRecId.slot);
		AttrCacheTable::recordToAttrCatEntry(Record,&(attrcacheEntry->attrCatEntry));
		attrcacheEntry->recId.block=attrcatRecId.block;
		attrcacheEntry->recId.slot=attrcatRecId.slot;
		attrcacheEntry->dirty=false;
		attrcacheEntry->next=listHead;
		listHead=attrcacheEntry;
	}
	AttrCacheTable::attrCache[relId]=listHead;
	// set the relIdth entry of the AttrCacheTable to listHead.

	/****** Setting up metadata in the Open Relation Table for the relation******/
	// update the relIdth entry of the tableMetaInfo with free as false and
	// relName as the input.
	
	OpenRelTable::tableMetaInfo[relId].free=false;
	strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName);
	return relId;
}


int OpenRelTable::closeRel(int relId)
{
	if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
	{
		return E_NOTPERMITTED;
	}

	if (relId < 0 || relId >= MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if (tableMetaInfo[relId].free)
	{
		return E_RELNOTOPEN;
	}

	// free the memory allocated in the relation and attribute caches which was
	// allocated in the OpenRelTable::openRel() function

	// update `tableMetaInfo` to set `relId` as a free slot
	// update `relCache` and `attrCache` to set the entry at `relId` to nullptr
	if (RelCacheTable::relCache[relId] != nullptr)
	{
		free(RelCacheTable::relCache[relId]);
		RelCacheTable::relCache[relId] = nullptr;
	}
	
	AttrCacheEntry* current = AttrCacheTable::attrCache[relId];
	while (current != nullptr)
	{
		AttrCacheEntry* temp = current;
		current = current->next;
		free(temp);
	}
	AttrCacheTable::attrCache[relId] = nullptr;
	
	tableMetaInfo[relId].free = true;
	tableMetaInfo[relId].relName[0] = '\0';

	return SUCCESS;
}

///the one in stage 8

/*int OpenRelTable::closeRel(int relId)
{
	if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
	{
		return E_NOTPERMITTED;
	}

	if (relId < 0 || relId >= MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if (tableMetaInfo[relId].free)
	{
		return E_RELNOTOPEN;
	}

	if(RelCacheTable::relCache[relId]->dirty==true)
	{
		Attribute record[RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry),record);
		RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
		relCatBlock.setRecord(record, RelCacheTable::relCache[relId]->recId.slot);
	}
	
	if (RelCacheTable::relCache[relId] != nullptr)
	{
		free(RelCacheTable::relCache[relId]);
		RelCacheTable::relCache[relId] = nullptr;
	}
	
	AttrCacheEntry* current = AttrCacheTable::attrCache[relId];
	while (current != nullptr)
	{
		AttrCacheEntry* temp = current;
		current = current->next;
		free(temp);
	}
	tableMetaInfo[relId].free = true;
	RelCacheTable::relCache[relId] = nullptr;
	AttrCacheTable::attrCache[relId] = nullptr;

	return SUCCESS;
}*/
