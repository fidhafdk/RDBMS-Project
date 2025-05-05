#include "Algebra.h"
#include <cstring>
#include<cstdlib>
#include<cstdio>

bool isNumber(char *str);
/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{	
	int srcRelId = OpenRelTable::getRelId(srcRel);      // we'll implement this later
	if (srcRelId == E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	AttrCatEntry  attrCatEntry;
	// get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
	//    return E_ATTRNOTEXIST if it returns the error
	if ( AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry)==E_ATTRNOTEXIST )
		return E_ATTRNOTEXIST;
	
	/*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
	int type = attrCatEntry.attrType;
	Attribute attrVal;
	if (type == NUMBER)
	{
		if (isNumber(strVal))
		{       // the isNumber() function is implemented below
			attrVal.nVal = atof(strVal);
		}
		else
		{
			return E_ATTRTYPEMISMATCH;
		}
	}
	else if (type == STRING)
	{
		strcpy(attrVal.sVal, strVal);
	}

	/*** Selecting records from the source relation ***/

	// Before calling the search function, reset the search to start from the first hit
	// using RelCacheTable::resetSearchIndex()
	//RelCacheTable::resetSearchIndex(crcRelId);
	
	RelCatEntry relCatEntry;
	// get relCatEntry using RelCacheTable::getRelCatEntry()
	RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
	int src_nAttrs = relCatEntry.numAttrs ;
	
	/* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
        (will store the attribute names of rel). */
        char attr_names[src_nAttrs][ATTR_SIZE];
    	// let attr_types[src_nAttrs] be an array of type int
    	int attr_types[src_nAttrs];
    	
	/*iterate through 0 to src_nAttrs-1 :
	get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
	fill the attr_names, attr_types arrays that we declared with the entries
	of corresponding attributes
	*/
	for (int i = 0; i < src_nAttrs; ++i)
	{
		AttrCatEntry attrCatEntry;
		// get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
		AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

		strcpy(attr_names[i],attrCatEntry.attrName);
		attr_types[i]=attrCatEntry.attrType;
	}
	
	/* Create the relation for target relation by calling Schema::createRel()
	by providing appropriate arguments */
	// if the createRel returns an error code, then return that value.
	int ret=Schema::createRel(targetRel, src_nAttrs, attr_names,attr_types);
	if(ret!=SUCCESS)
		return ret;
		
	/* Open the newly created target relation by calling OpenRelTable::openRel()
	method and store the target relid */
	int targetRelId=OpenRelTable::openRel(targetRel);
	/* If opening fails, delete the target relation by calling Schema::deleteRel()
	and return the error value returned from openRel() */
	if(targetRelId<0 || targetRelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetRelId;
	}
	/*** Selecting and inserting records into the target relation ***/
	/* Before calling the search function, reset the search to start from the
	first using RelCacheTable::resetSearchIndex() */
	
	Attribute record[src_nAttrs];
	
	/*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
	*/
	RelCacheTable::resetSearchIndex(srcRelId);
	AttrCacheTable::resetSearchIndex(srcRelId,attr);

	// read every record that satisfies the condition by repeatedly calling
	// BlockAccess::search() until there are no more records to be read
	
	while (BlockAccess::search(srcRelId, record, attr,attrVal, op)==SUCCESS)
	{

		ret = BlockAccess::insert(targetRelId, record);

		if (ret!=SUCCESS)
		{
			Schema::closeRel(targetRel);
			Schema::deleteRel(targetRel);
			return ret;
		}
	}

	// Close the targetRel by calling closeRel() method of schema layer
	Schema::closeRel(targetRel);

	return SUCCESS;
}

// will return if a string can be parsed as a floating point number
bool isNumber(char *str)
{
	int len;
	float ignore;
	/*
	sscanf returns the number of elements read, so if there is no float matching
	the first %f, ret will be 0, else it'll be 1

	%n gets the number of characters read. this scanf sequence will read the
	first float ignoring all the whitespace before and after. and the number of
	characters read that far will be stored in len. if len == strlen(str), then
	the string only contains a float with/without whitespace. else, there's other
	characters.
	*/
	int ret = sscanf(str, "%f %n", &ignore, &len);
	return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
	if(strcmp(relName,RELCAT_RELNAME)==0 ||strcmp(relName,ATTRCAT_RELNAME)==0)
		return E_NOTPERMITTED;
		
	int relId = OpenRelTable::getRelId(relName);

	if (relId==E_RELNOTOPEN)
		return E_RELNOTOPEN;
	// get the relation catalog entry from relation cache
	// (use RelCacheTable::getRelCatEntry() of Cache Layer)
	RelCatEntry relcatentry;
	RelCacheTable::getRelCatEntry(relId,&relcatentry);

	/* if relCatEntry.numAttrs != numberOfAttributes in relation,
	return E_NATTRMISMATCH */
	if(relcatentry.numAttrs!=nAttrs)
		return E_NATTRMISMATCH;

	// let recordValues[numberOfAttributes] be an array of type union Attribute
	union Attribute recordValues[nAttrs];
	/*
	Converting 2D char array of record values to Attribute array recordValues
	*/
	for(int i=0;i<nAttrs;++i)
	{
		// get the attr-cat entry for the i'th attribute from the attr-cache
		// (use AttrCacheTable::getAttrCatEntry())
		AttrCatEntry attrcatentry;
		AttrCacheTable::getAttrCatEntry(relId, i, &attrcatentry);
		// let type = attrCatEntry.attrType;
		int type=attrcatentry.attrType;
		if (type==NUMBER)
		{
			// if the char array record[i] can be converted to a number
			// (check this using isNumber() function)
			if(isNumber(record[i]))
			{
				/* convert the char array to numeral and store it
				at recordValues[i].nVal using atof() */
				recordValues[i].nVal=atof(record[i]);
			}
			else
			{
				return E_ATTRTYPEMISMATCH;
			}
		}
		else if (type == STRING)
		{
			// copy record[i] to recordValues[i].sVal
			strcpy(recordValues[i].sVal,record[i]);
		}
	}
	// insert the record by calling BlockAccess::insert() function
	// let retVal denote the return value of insert call
	int ret=BlockAccess::insert(relId, recordValues);
	return ret;

}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);

	// if srcRel is not open in open relation table, return E_RELNOTOPEN
	if (srcRelId < 0 || srcRelId >= MAX_OPEN)
		return E_RELNOTOPEN;
		
	RelCatEntry RelCatEntrySrcRel;
	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCacheTable::getRelCatEntry(srcRelId,&RelCatEntrySrcRel);
	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int numAttrs=RelCatEntrySrcRel.numAttrs;

	// attrNames and attrTypes will be used to store the attribute names
	// and types of the source relation respectively
	char attrNames[numAttrs][ATTR_SIZE];
	int attrTypes[numAttrs];

	/*iterate through every attribute of the source relation :
	- get the AttributeCat entry of the attribute with offset.
	(using AttrCacheTable::getAttrCatEntry())
	- fill the arrays `attrNames` and `attrTypes` that we declared earlier
	with the data about each attribute
	*/
	
	for(int i=0;i<numAttrs;++i)
	{
		AttrCatEntry AttrCatEntrySrcRel;
		AttrCacheTable::getAttrCatEntry(srcRelId, i, &AttrCatEntrySrcRel);
		strcpy(attrNames[i],AttrCatEntrySrcRel.attrName);
		attrTypes[i]=AttrCatEntrySrcRel.attrType;
	}
		
	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()
	int ret=Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
	if(ret!=SUCCESS)
		return ret;
	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int targetrelId=OpenRelTable::openRel(targetRel);

	// If opening fails, delete the target relation by calling Schema::deleteRel() of
	// return the error value returned from openRel().
	if(targetrelId<0 || targetrelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetrelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);
	Attribute record[numAttrs];


	while ( BlockAccess::project(srcRelId, record)==SUCCESS)
	{
		// record will contain the next record

		ret = BlockAccess::insert(targetrelId, record);

		if (ret!=SUCCESS)
		{
			// close the targetrel by calling Schema::closeRel()
			Schema::closeRel(targetRel);
			// delete targetrel by calling Schema::deleteRel()
			Schema::deleteRel(targetRel);
			return ret;
		}
	}

	// Close the targetRel by calling Schema::closeRel()
	Schema::closeRel(targetRel);

	return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);

	// if srcRel is not open in open relation table, return E_RELNOTOPEN
	if (srcRelId < 0 || srcRelId >= MAX_OPEN)
		return E_RELNOTOPEN;

	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCatEntry RelCatEntrySrcRel;
	RelCacheTable::getRelCatEntry(srcRelId,&RelCatEntrySrcRel);

	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int src_nAttrs= RelCatEntrySrcRel.numAttrs;

	// declare attr_offset[tar_nAttrs] an array of type int.
	// where i-th entry will store the offset in a record of srcRel for the
	// i-th attribute in the target relation.
	int attr_offset[tar_nAttrs];

	// let attr_types[tar_nAttrs] be an array of type int.
	// where i-th entry will store the type of the i-th attribute in the
	// target relation.
	int attr_types[tar_nAttrs];

	/*** Checking if attributes of target are present in the source relation
	and storing its offsets and types ***/

	/*iterate through 0 to tar_nAttrs-1 :
	- get the attribute catalog entry of the attribute with name tar_attrs[i].
	- if the attribute is not found return E_ATTRNOTEXIST
	- fill the attr_offset, attr_types arrays of target relation from the
	corresponding attribute catalog entries of source relation
	*/
	for(int i=0; i<tar_nAttrs; ++i)
	{
		AttrCatEntry AttrCatEntrySrcRel;
		int ret=AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &AttrCatEntrySrcRel);
		if(ret== E_ATTRNOTEXIST)
			return ret;
		attr_offset[i]=AttrCatEntrySrcRel.offset;
		attr_types[i]=AttrCatEntrySrcRel.attrType;
	}
	
	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()
	int ret=Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
	if(ret!=SUCCESS)
		return ret;
	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int targetRelId=OpenRelTable::openRel(targetRel);
	if(targetRelId<0 || targetRelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return targetRelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);

	Attribute record[src_nAttrs];

	while (BlockAccess::project(srcRelId, record)==SUCCESS)
	{
		// the variable `record` will contain the next record

		Attribute proj_record[tar_nAttrs];

		//iterate through 0 to tar_attrs-1:
		//    proj_record[attr_iter] = record[attr_offset[attr_iter]]
		for(int i=0; i<tar_nAttrs; ++i)
		{
			 proj_record[i] = record[attr_offset[i]];
		}
		

		ret = BlockAccess::insert(targetRelId, proj_record);

		if (ret!=SUCCESS)
		{
			// close the targetrel by calling Schema::closeRel()
			Schema::closeRel(targetRel);
			// delete targetrel by calling Schema::deleteRel()
			Schema::deleteRel(targetRel);
			return ret;
		}
	}
	// Close the targetRel by calling Schema::closeRel()
	Schema::closeRel(targetRel);

	return SUCCESS;
}


int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE])
{

	// get the srcRelation1's rel-id using OpenRelTable::getRelId() method
	// get the srcRelation2's rel-id using OpenRelTable::getRelId() method
	
	int srcRel1Id=OpenRelTable::getRelId(srcRelation1);
	int srcRel2Id=OpenRelTable::getRelId(srcRelation2);

	// if either of the two source relations is not open
	//     return E_RELNOTOPEN
	if(srcRel1Id==E_RELNOTOPEN || srcRel2Id==E_RELNOTOPEN)
		return E_RELNOTOPEN;

	AttrCatEntry attrCatEntry1, attrCatEntry2;
	// get the attribute catalog entries for the following from the attribute cache
	// (using AttrCacheTable::getAttrCatEntry())
	// - attrCatEntry1 = attribute1 of srcRelation1
	// - attrCatEntry2 = attribute2 of srcRelation2
//	printf("%s %s\n",attribute1, attribute2);
	int ret1=AttrCacheTable::getAttrCatEntry(srcRel1Id, attribute1, &attrCatEntry1);
	int ret2=AttrCacheTable::getAttrCatEntry(srcRel2Id, attribute2, &attrCatEntry2);
//	printf("%s %s\n",attrCatEntry1.attrName, attrCatEntry2.attrName);
//	printf("Checking attrs\n");
	
	if(ret1!=SUCCESS || ret2!=SUCCESS)
		return E_ATTRNOTEXIST;
//	printf("Checked attrs\n");

	// if attribute1 and attribute2 are of different types return E_ATTRTYPEMISMATCH
	if(attrCatEntry1.attrType != attrCatEntry2.attrType)
		return E_ATTRTYPEMISMATCH;
		
	// get the relation catalog entries for the relations from the relation cache
	// (use RelCacheTable::getRelCatEntry() function)
	RelCatEntry relCatEntry1, relCatEntry2;
	RelCacheTable::getRelCatEntry(srcRel1Id, &relCatEntry1);
	RelCacheTable::getRelCatEntry(srcRel2Id, &relCatEntry2);

	int numOfAttributes1 = relCatEntry1.numAttrs;
	int numOfAttributes2 = relCatEntry2.numAttrs;

	// iterate through all the attributes in both the source relations and check if
	// there are any other pair of attributes other than join attributes
	// (i.e. attribute1 and attribute2) with duplicate names in srcRelation1 and
	// srcRelation2 (use AttrCacheTable::getAttrCatEntry())
	// If yes, return E_DUPLICATEATTR
	for(int i=0; i<numOfAttributes1 ; ++i)
	{
		AttrCatEntry attrcatentry1;
		AttrCacheTable::getAttrCatEntry(srcRel1Id, i, &attrcatentry1);
		if(strcmp(attrcatentry1.attrName,attribute1)==0)
				continue;
		for(int j=0; j<numOfAttributes2 ; ++j)
		{
			AttrCatEntry attrcatentry2;
			AttrCacheTable::getAttrCatEntry(srcRel2Id, j, &attrcatentry2);
			if(strcmp(attrcatentry2.attrName,attribute2)==0)
				continue;
			if(strcmp(attrcatentry1.attrName, attrcatentry2.attrName)==0)
			{
				return E_DUPLICATEATTR;
			}
		}
	}

	

	
	// if rel2 does not have an index on attr2
	//     create it using BPlusTree:bPlusCreate()
	if(attrCatEntry2.rootBlock==-1)
	{
		ret1=BPlusTree::bPlusCreate(srcRel2Id, attribute2);
		if(ret1!=SUCCESS)
			return ret1;
	}


	int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;


	// declare the following arrays to store the details of the target relation
	char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
	int targetRelAttrTypes[numOfAttributesInTarget];

	// iterate through all the attributes in both the source relations and
	// update targetRelAttrNames[],targetRelAttrTypes[] arrays excluding attribute2
	// in srcRelation2 (use AttrCacheTable::getAttrCatEntry())
	for(int i=0; i<numOfAttributes1 ; ++i)
	{
		AttrCatEntry attrcatentry1;
		AttrCacheTable::getAttrCatEntry(srcRel1Id, i, &attrcatentry1);
		strcpy(targetRelAttrNames[i],attrcatentry1.attrName);
		targetRelAttrTypes[i]=attrcatentry1.attrType;
		
	}
	int k=0;
	for(int j=0; j<numOfAttributes2 ; ++j)
	{
		AttrCatEntry attrcatentry2;
		AttrCacheTable::getAttrCatEntry(srcRel2Id, j, &attrcatentry2);
		if(strcmp(attrcatentry2.attrName,attribute2)==0)
		{
			k=1;
			continue;
		}
		strcpy(targetRelAttrNames[j+numOfAttributes1-k],attrcatentry2.attrName);
		targetRelAttrTypes[j+numOfAttributes1-k]=attrcatentry2.attrType;
		
	}

	// create the target relation using the Schema::createRel() function

	// if createRel() returns an error, return that error
	ret1=Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
	if(ret1!=SUCCESS)
		return ret1;
	// Open the targetRelation using OpenRelTable::openRel()
	int newrelId=OpenRelTable::openRel(targetRelation);
	if(newrelId<0 || newrelId>=MAX_OPEN)
	{
		Schema::deleteRel(targetRelation);
		return newrelId;
	}

	Attribute record1[numOfAttributes1];
	Attribute record2[numOfAttributes2];
	Attribute targetRecord[numOfAttributesInTarget];

	RelCacheTable::resetSearchIndex(srcRel1Id);
	// this loop is to get every record of the srcRelation1 one by one
	while (BlockAccess::project(srcRel1Id, record1) == SUCCESS)
	{

		// reset the search index of `srcRelation2` in the relation cache
		// using RelCacheTable::resetSearchIndex()
		// reset the search index of `attribute2` in the attribute cache
		// using AttrCacheTable::resetSearchIndex()
		AttrCacheTable::resetSearchIndex(srcRel2Id, attribute2);
		RelCacheTable::resetSearchIndex(srcRel2Id);

		
		// this loop is to get every record of the srcRelation2 which satisfies
		//the following condition:
		// record1.attribute1 = record2.attribute2 (i.e. Equi-Join condition)
		while (BlockAccess::search(
		    srcRel2Id, record2, attribute2, record1[attrCatEntry1.offset], EQ
		) == SUCCESS )
		{

			// copy srcRelation1's and srcRelation2's attribute values(except
			// for attribute2 in rel2) from record1 and record2 to targetRecord
			for(int i=0; i<numOfAttributes1 ; ++i)
			{
				targetRecord[i]=record1[i];
			}
			int k=0;
			for(int j=0; j<numOfAttributes2 ; ++j)
			{
				if(j==attrCatEntry2.offset)
				{
					k=1;
					continue;
				}
				targetRecord[j+numOfAttributes1-k]=record2[j];
			}
			
	

			// insert the current record into the target relation by calling
			// BlockAccess::insert()
			ret1=BlockAccess::insert(newrelId, targetRecord);

			if(ret1!=SUCCESS)
			{

				// close the target relation by calling OpenRelTable::closeRel()
				// delete targetRelation (by calling Schema::deleteRel())
				OpenRelTable::closeRel(newrelId);
				Schema::deleteRel(targetRelation);
				return E_DISKFULL;
			}
		}
	}

	// close the target relation by calling OpenRelTable::closeRel()
	OpenRelTable::closeRel(newrelId);
	return SUCCESS;
}
