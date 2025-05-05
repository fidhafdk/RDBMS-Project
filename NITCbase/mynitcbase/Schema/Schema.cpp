#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE])
{
	int ret = OpenRelTable::openRel(relName);

	// the OpenRelTable::openRel() function returns the rel-id if successful
	// a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
	// error codes will be negative
	if(ret >= 0)
	{
		return SUCCESS;
	}

	//otherwise it returns an error message
	return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
	if (strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
	{
		return E_NOTPERMITTED;
	}

	// this function returns the rel-id of a relation if it is open or
	// E_RELNOTOPEN if it is not. we will implement this later.
	int relId = OpenRelTable::getRelId(relName);

	if (relId==E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
	// if the oldRelName or newRelName is either Relation Catalog or Attribute Catalog,
	// return E_NOTPERMITTED
	// (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
	// you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
	if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
	strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;
	
	// if the relation is open
	//    (check if OpenRelTable::getRelId() returns E_RELNOTOPEN)
	//    return E_RELOPEN
	if (OpenRelTable::getRelId(oldRelName)!=E_RELNOTOPEN)
		return E_RELOPEN;
	
	return BlockAccess::renameRelation(oldRelName, newRelName);
	
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName)
{
	// if the relName is either Relation Catalog or Attribute Catalog,
	// return E_NOTPERMITTED
	// (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
	// you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;

	// if the relation is open
	//    (check if OpenRelTable::getRelId() returns E_RELNOTOPEN)
	//    return E_RELOPEN
	if (OpenRelTable::getRelId(relName)!=E_RELNOTOPEN)
		return E_RELOPEN;
	// Call BlockAccess::renameAttribute with appropriate arguments.
	// return the value returned by the above renameAttribute() call
	return BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);
		
}

int Schema::deleteRel(char *relName)
{
	if (strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0)
		return E_NOTPERMITTED;

	// get the rel-id using appropriate method of OpenRelTable class by
	// passing relation name as argument
	int ret=OpenRelTable::getRelId(relName);
	if (ret>=0 && ret<MAX_OPEN)
		return E_RELOPEN;

	// Call BlockAccess::deleteRelation() with appropriate argument.
	ret=BlockAccess::deleteRelation(relName);
	return ret;

}

int Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE],int attrtype[])
{
	Attribute relNameAsAttribute;
	strcpy(relNameAsAttribute.sVal,relName);

	RecId targetRelId;

	RelCacheTable::resetSearchIndex(RELCAT_RELID);
	char RelName[8];
	strcpy(RelName,"RelName");
	targetRelId=BlockAccess::linearSearch(RELCAT_RELID,RelName, relNameAsAttribute, EQ);

	if (targetRelId.block!=-1 || targetRelId.slot!=-1)
		return E_RELEXIST;

	// compare every pair of attributes of attrNames[] array
	// if any attribute names have same string value,
	//     return E_DUPLICATEATTR (i.e 2 attributes have same value)
	for(int i=0; i<nAttrs; ++i)
	{
		for(int j=i+1; j<nAttrs;++j)
		{
			if(strcmp(attrs[i],attrs[j])==0)
				return E_DUPLICATEATTR;
		}
	}

	/* declare relCatRecord of type Attribute which will be used to store the
	record corresponding to the new relation which will be inserted
	into relation catalog */
	Attribute relCatRecord[RELCAT_NO_ATTRS];

	strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,relName);
	relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal=nAttrs;
	relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal=0;
	relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal=-1;
	relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal=-1;
	int numofslots=floor(2016 / ((16 * nAttrs) + 1));
	relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal=numofslots;
	
	
	
	int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
	if( retVal!=SUCCESS)
		return retVal;

	for(int i=0; i<nAttrs; ++i)
	{
		/* declare Attribute attrCatRecord[6] to store the attribute catalog
		record corresponding to i'th attribute of the argument passed*/
		Attribute attrCatRecord[6];
		
		strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relName);
		strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,attrs[i]);
		attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal=attrtype[i];
		attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal=-1;
		attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal=-1;
		attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal=i;
		retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
		/* if attribute catalog insert fails:
		delete the relation by calling deleteRel(targetrel) of schema layer
		return E_DISKFULL
		// (this is necessary because we had already created the
		//  relation catalog entry which needs to be removed)
		*/
		if( retVal!=SUCCESS)
		{
			Schema::deleteRel(relName);
			return E_DISKFULL;
		}
	}
	

	return SUCCESS;
}


