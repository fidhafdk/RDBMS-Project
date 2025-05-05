#include "AttrCacheTable.h"
#include <cstring>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

/* returns the attrOffset-th attribute for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf)
{
	// check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
	if(relId<0 || relId >=MAX_OPEN)
		return E_OUTOFBOUND;

	// check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
	if(attrCache[relId] == nullptr)
		return E_RELNOTOPEN;

	// traverse the linked list of attribute cache entries
	for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next)
	{
		if (entry->attrCatEntry.offset==attrOffset)
		{

		      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
		      *attrCatBuf=entry->attrCatEntry;
		      return SUCCESS;
		}
	}

	  // there is no attribute at this offset
	return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf)
{
	// check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
	if(relId<0 || relId >=MAX_OPEN)
		return E_OUTOFBOUND;

	// check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
	if(attrCache[relId] == nullptr)
		return E_RELNOTOPEN;

	// traverse the linked list of attribute cache entries
	for(AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next)
	{
		if (strcmp(entry->attrCatEntry.attrName,attrName)==0)
		{

		      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
		      *attrCatBuf=entry->attrCatEntry;
		      return SUCCESS;
		}
	}

	  // there is no attribute at this offset
	return E_ATTRNOTEXIST;
}

/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],AttrCatEntry* attrCatEntry)
{
	strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);

	// copy the rest of the fields in the record to the attrCacheEntry struct
	strcpy(attrCatEntry->attrName,record[ATTRCAT_ATTR_NAME_INDEX].sVal);
	attrCatEntry->attrType=record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
	attrCatEntry->primaryFlag=record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
	attrCatEntry->rootBlock=record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
	attrCatEntry->offset=record[ATTRCAT_OFFSET_INDEX].nVal;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, Attribute record[ATTRCAT_NO_ATTRS])
{
	strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
	strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
	record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
	record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
	record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
	record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;

}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE])
{
	IndexId index;
	index.block = -1;
	index.index = -1;
	return AttrCacheTable::setSearchIndex(relId,attrName, &index);
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset)
{
	IndexId index;
	index.block = -1;
	index.index = -1;
	return AttrCacheTable::setSearchIndex(relId, attrOffset, &index);
}

int  AttrCacheTable:: setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
	if(relId<0 || relId>=MAX_OPEN)
		return E_OUTOFBOUND;
  
	if(AttrCacheTable::attrCache[relId]==nullptr)
		return E_RELNOTOPEN;

	AttrCacheEntry *curr=AttrCacheTable::attrCache[relId];
	while(curr)
	{
		if(strcmp(curr->attrCatEntry.attrName,attrName)==0)
		{
			curr->searchIndex=*searchIndex;
			return SUCCESS;
		}
		curr=curr->next;
	}
	return E_ATTRNOTEXIST;
}

int  AttrCacheTable:: setSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
	if(relId<0 || relId>=MAX_OPEN)
		return E_OUTOFBOUND;
  
	if(AttrCacheTable::attrCache[relId]==nullptr)
		return E_RELNOTOPEN;

	AttrCacheEntry *curr=AttrCacheTable::attrCache[relId];
	while(curr)
	{
		if(attrOffset==curr->attrCatEntry.offset)
		{
			curr->searchIndex=*searchIndex;
			return SUCCESS;
		}
		curr=curr->next;
	}
	return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{

	if(relId<0 ||relId>=MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if(AttrCacheTable::attrCache[relId] == nullptr)
	{
		return E_RELNOTOPEN;
	}

	
	for(AttrCacheEntry* temp=AttrCacheTable::attrCache[relId];temp!=nullptr;temp=temp->next)
	{
		if (strcmp(attrName,temp->attrCatEntry.attrName)==0)
		{
			//copy the searchIndex field of the corresponding Attribute Cache entry
			//in the Attribute Cache Table to input searchIndex variable.
			*searchIndex=temp->searchIndex;
			return SUCCESS;
		}
	}

	return E_ATTRNOTEXIST;
}
	

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{

	if(relId<0 ||relId>=MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if(AttrCacheTable::attrCache[relId] == nullptr)
	{
		return E_RELNOTOPEN;
	}

	
	for(AttrCacheEntry* temp=attrCache[relId];temp!=nullptr;temp=temp->next)
	{
		if (attrOffset==temp->attrCatEntry.offset)
		{
			//copy the searchIndex field of the corresponding Attribute Cache entry
			//in the Attribute Cache Table to input searchIndex variable.
			*searchIndex=temp->searchIndex;
			return SUCCESS;
		}
	}

	return E_ATTRNOTEXIST;
}	


int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf)
{

	if(relId<0 || relId>=MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if(AttrCacheTable::attrCache[relId]==nullptr)
	{
		return E_RELNOTOPEN;
	}

	for(AttrCacheEntry *current=AttrCacheTable::attrCache[relId]; current!=nullptr; current=current->next)
	{
		if(strcmp(current->attrCatEntry.attrName,attrName)==0)
		{
			// copy the attrCatBuf to the corresponding Attribute Catalog entry in
			// the Attribute Cache Table.
			current->attrCatEntry=*attrCatBuf;
			current->dirty=true;
			return SUCCESS;
		}
	}

	return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf)
{

	if(relId<0 || relId>=MAX_OPEN)
	{
		return E_OUTOFBOUND;
	}

	if(AttrCacheTable::attrCache[relId]==nullptr)
	{
		return E_RELNOTOPEN;
	}

	for(AttrCacheEntry *current=AttrCacheTable::attrCache[relId]; current!=nullptr; current=current->next)
	{
		if( current->attrCatEntry.offset==attrOffset)
		{
			// copy the attrCatBuf to the corresponding Attribute Catalog entry in
			// the Attribute Cache Table.
			current->attrCatEntry=*attrCatBuf;
			current->dirty=true;
			return SUCCESS;
		}
	}

	return E_ATTRNOTEXIST;
}















