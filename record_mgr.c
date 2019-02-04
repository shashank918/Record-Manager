#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

                                                              
char unit=sizeof(char);
int INT=sizeof(int);

BM_PageHandle *ph;	 
BM_BufferPool *bm;	 
RID *rid;
char *Temp;

//DataStructure to keep track of table
typedef struct tableData
{
    int numOfTuples;	        
    int freeSlot;	          
    char *name;               
    BM_PageHandle handlePages;	
    BM_BufferPool poolHandle;	
}tableData;
tableData *ts;

//DataStructure to Scan tuples in table
typedef struct scanTuple
{
    Expr *Condition;        
    RID key;                
    BM_PageHandle handlePages;
} scanTuple;

SM_FileHandle handleFile;
SM_PageHandle handlePage;

#define INCREMENT_PH() handlePage += 4;

						/************** TABLE AND MANAGER ************/

/* function Name: initRecordManager
 * functionality: Initializes and record manager.
 */
RC initRecordManager (void *mgmtData)
{
rc(RC_OK); //we don't have to initialize the record manager
}

/* function Name: shutdownRecordManager
 * functionality: Shut down the present record manager 
 */
RC shutdownRecordManager ()
{
rc(RC_OK);//we don't have to shout down the record manager
}

/* function Name: CReateTable
 * functionality: creates new page file for given table and stores the schema in first page.
 */
RC createTable (char *name, Schema *schema)
{          
char pageSize[PAGE_SIZE];
int i=0;                                                   
Allocatemem(ts,tableData)
MOVE(bm,&ts->poolHandle)
initBufferPool(bm, name, 50, RS_FIFO, NULL);              

char *handlePages = pageSize;

FORLOOP(i,i<PAGE_SIZE)
statements(handlePages[i]=0;)
FORLOOP(i,i<=unit)   
statements(*(int*)handlePages = i;handlePages += 4;)                                         
                                                           
MOVE(*(int*)handlePages,3)                                     
INC(handlePages,4)                                           
MOVE(*(int*)handlePages,(int)schema->dataTypes[i])             
INC(handlePages,unit)                                   
MOVE(*(int*)handlePages,(int) schema->typeLength[i])      
INC(handlePages,unit)                                   

check(name != NULL)        
statements(createPageFile(name);openPageFile(name, &handleFile);writeBlock(0, &handleFile, pageSize);)                        
doelse   
statements(rc(RC_FILE_NOT_FOUND);)  
rc(RC_OK);
}

/* function Name: OpenTable
 * functionality: Access the table by accessing the page file using buffer manager .
 */
RC openTable (RM_TableData *rel, char *name)
{
int i=0;
MOVE(rel->mgmtData,ts)        
MOVE(ph,&ts->handlePages)   
MOVE(bm,&ts->poolHandle)                                
pinPage(bm, ph, -1);                  
MOVE(handlePage,(char*) ts->handlePages.data)	                             
MOVE(ts->numOfTuples,*(int*)handlePage)	  
INCREMENT_PH();
MOVE(ts->freeSlot,*(int*)handlePage)	      
INCREMENT_PH();
MOVE(i,*(int*)handlePage)		              
INCREMENT_PH();

Schema *sh;
Allocatemem(sh,Schema)
MOVE(sh->numAttr,i)
sh->attrNames= (char**) malloc(sizeof(char*)*3);
sh->dataTypes= (DataType*) malloc(INT*3); 
sh->typeLength= (int*) malloc(INT*3);  

FORLOOP(i,i<= unit)    
statements(sh->attrNames[i]=(char*)malloc(unit+INT);)

for(i=i; i<= (unit+INT); i++) 
MOVE(sh->dataTypes[i],*(int*)handlePage)      

MOVE(rel->schema,sh) 
unpinPage(bm, ph);                     
rc(RC_OK);  
}

/* function Name: closeTable
 * functionality: closes the table buy shut downing buffer pool .
 */   
RC closeTable (RM_TableData *rel)
{
BM_BufferPool *poolHandle=(BM_BufferPool *)rel->mgmtData;
MOVE(ts,rel->mgmtData)    
MOVE(bm,&ts->poolHandle)                         
shutdownBufferPool(bm);	                       
rc(RC_OK);
}



/* function Name: deleteTable

 * functionality: delets the table by destroying the associated pages of file.

 */  

RC deleteTable (char *name)

{

destroyPageFile(name);

return RC_OK;

}

 

/* function Name: getNumTuples

 * functionality: returns the number of tuples of a table.

 */

int getNumTuples (RM_TableData *rel)                             

{

MOVE(ts,(tableData*)rel->mgmtData)

rc(ts->numOfTuples);

}

 

                                                          /*************** HANDLING RECORDS IN A TABLE ********************/

/* function Name: getFreeSlot

 * functionality: returns the available free slot.

 */ 

int getFreeSlot(char *v, int size)

{

int i, totalSlots;

MOVE(totalSlots,ceil(PAGE_SIZE/size));

for(i=0; i<totalSlots; i++)

{

              if (v[i*size] != '$')

                             return i;

}

return -1;

}

 

/* function Name: insertRecord

 * functionality: will insert new record in available page and slot and will assign that to record parameter.

 */ 

RC insertRecord (RM_TableData *rel, Record *record)                                    

{

MOVE(ts,rel->mgmtData);                                                                                 

MOVE(rid,&record->id);                             

MOVE(rid->page,ts->freeSlot);                                             

MOVE(bm,&ts->poolHandle);

MOVE(ph,&ts->handlePages);

int recordSize;

MOVE(recordSize,getRecordSize(rel->schema));

int i = rid->page, j;

pinPage(bm, ph, i);           

 

char *data = ts->handlePages.data;     

 

rid->slot = getFreeSlot(data, recordSize);                                         

MOVE(j,rid->slot);

 

while(j<0)                                                                                                   

{                                     

i++;                                                                                                               

pinPage(bm, ph, i);        

MOVE(data,ts->handlePages.data);        

MOVE(j,getFreeSlot(data, recordSize)); 

}

 

markDirty(bm,ph);                                         

 

data += (j * recordSize);              

MOVE(*data,'$');                                                                     

data++;

   MOVE(rid->slot,j);

MOVE(rid->page,i);

memmove(data, record->data+1, recordSize);

unpinPage(bm, ph);                                      

pinPage(bm, ph, 1);                         

return RC_OK;                                                                                                           

}

 

/* function Name: deleteRecord

 * functionality: will delete the record based on the RID parameter that is passed.

 */ 

RC deleteRecord (RM_TableData *rel, RID id)                                              

{

              MOVE(bm,&ts->poolHandle);

              MOVE(ph,&ts->handlePages);

              int tuples, recordSize;

             

              openPageFile(ts->name, &handleFile);                

 

              int rc;

  MOVE(rc,pinPage(bm, ph,id.page));

 

              if(rc == RC_OK){   

                             MOVE(Temp,ts->handlePages.data);

              }else{

                             return RC_WRITE_FAILED;

              }

              MOVE(tuples,ts->numOfTuples);                           

              MOVE(recordSize,getRecordSize(rel->schema));

              tuples += (id.slot * recordSize);                

              markDirty(bm, ph);                          

              unpinPage(bm, ph);                                   

 

              return RC_OK;                                                                                                         

}

 

/* function Name: updateRecord

 * functionality: will update  an  existing record with a new value on the slot and page that has been passed.

 */

RC updateRecord (RM_TableData *rel, Record *record)                                                                                       

{

              MOVE(bm,&ts->poolHandle);

              MOVE(ph,&ts->handlePages);

              MOVE(rid,&record->id);                            

              int page, recordSize;

                             MOVE(page,record->id.page);

             

              pinPage(bm, ph, page);     

MOVE(recordSize,getRecordSize(rel->schema));                                           

MOVE(Temp,(ts->handlePages.data) + (rid->slot * recordSize));

Temp += 1;        

 

memmove(Temp,record->data + 1, recordSize);                             

markDirty(bm, ph);                          

unpinPage(bm, ph);                         

return RC_OK;                                                                                                           

}
/* function Name: getRecord
 * functionality: will get an  existing record value based on RID parameter(id) and assing the value to record plus assign that RID to record->id.
 */
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	MOVE(bm,&ts->poolHandle);
	MOVE(ph,&ts->handlePages);
	int page, recordSize;
	MOVE(page,id.page);
	char *pointer;

	pinPage(bm, ph, page);
	MOVE(recordSize,getRecordSize(rel->schema));
	MOVE(pointer,(ts->handlePages.data) + ((id.slot)*recordSize));
	MOVE(pointer,pointer +1);
	MOVE(Temp,record->data);
	MOVE(Temp,Temp + 1);
	memmove(Temp,pointer,recordSize - 1);
	MOVE(record->id,id);
	unpinPage(bm, ph);
	return RC_OK;
}

/* function Name: startScan
 * functionality: will scan 30 records at a time.
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

openTable(rel, Temp);

scanTuple *scanPtr;
MOVE(scanPtr,(scanTuple*) malloc(sizeof(scanTuple)));
MOVE(scanPtr->key.page,1);
MOVE(scanPtr->key.slot,0);
MOVE(scanPtr->Condition,cond);
MOVE(scan->mgmtData,scanPtr);
MOVE(ts,rel->mgmtData);
MOVE(ts->numOfTuples,30);
MOVE(scan->rel,rel);

return RC_OK;
}

/* function Name: next
 * functionality: will scan the next records that match the condition.
 */
RC next (RM_ScanHandle *scan, Record *record)
{
	scanTuple *scanPtr;
	Value *valPtr;
	int recordSize;
	char *ptr;

	MOVE(scanPtr,(scanTuple*)scan->mgmtData);
	MOVE(Schema *sptr,scan->rel->schema);
	MOVE(ts,(tableData*)scan->rel->mgmtData);

//Valptr initialized with m alloc for size of value
	MOVE(valPtr,(Value *) malloc(sizeof(Value)));
	MOVE(bm,&ts->poolHandle);
	MOVE(ph,&scanPtr->handlePages);
	MOVE(recordSize,getRecordSize(sptr));

	int i;
	for(i=0; i <= ts->numOfTuples; i++)
	{
	scanPtr->key.slot += 1;
	pinPage(bm, ph, scanPtr->key.page);
	MOVE(ptr,scanPtr->handlePages.data);
	ptr += (scanPtr->key.slot * recordSize);
	MOVE(record->id.page,scanPtr->key.page);
	MOVE(record->id.slot,scanPtr->key.slot);
	MOVE(Temp,record->data);
	Temp += 1;

	memmove(Temp,ptr+1,recordSize-1);
	evalExpr(record, sptr, scanPtr->Condition, &valPtr);

	if((valPtr->v.floatV == TRUE)||(valPtr->v.boolV == TRUE))
	{
	unpinPage(bm, ph);
	return RC_OK;
	}
	}

	unpinPage(bm, ph);
	return RC_RM_NO_MORE_TUPLES;
}

/* function Name: closeScan
 * functionality: will close a current scan.
 */
RC closeScan (RM_ScanHandle *scan)
{
	scanTuple *scanPtr;
	MOVE(scanPtr,(scanTuple*)scan->mgmtData);
	MOVE(bm,&ts->poolHandle);
	MOVE(ph,&scanPtr->handlePages);
	unpinPage(bm, ph);
	return RC_OK;
}

/* function Name: getRecordSize
 * functionality: will return the size of a record.
 */
int getRecordSize (Schema *schema)
{
	int i, Offset=0;

for(i=0; i<schema->numAttr; i++)
{
if (schema->dataTypes[i] == DT_INT)
	Offset += 4;
else if(schema->dataTypes[i] == DT_STRING)
	Offset += schema->typeLength[i];
}
Offset++;
return Offset;
}

/* function Name: createSchema
 * functionality: will create and initialise the new schema and returns the pointer to it.
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

Allocatemem(Schema *sh,Schema);
MOVE(sh->numAttr,numAttr);
MOVE(sh->attrNames,attrNames);
MOVE(sh->dataTypes,dataTypes);
MOVE(sh->typeLength,typeLength);
MOVE(sh->keySize,keySize);
MOVE(sh->keyAttrs,keys);
return sh;
}
 
 
/*freeSchema : deallocate the memory allocated to a schema.*/
RC freeSchema (Schema *schema)
{
free(schema);                                              
return RC_OK;
}
/* createRecord : creates a new record */
RC createRecord(Record **record, Schema *schema)
{               
int i, recordSize;
for(i=0; i < INT; i++)            
{
switch(*(schema->dataTypes + i))
{
case DT_INT:
case DT_FLOAT: recordSize += INT; 
break;   
case DT_BOOL: recordSize += 1; 
break;
case DT_STRING: recordSize += (*(schema->typeLength + i));  
break;
}
}
for(i=0; i < INT + unit; i++)        
{
Temp = (char *)malloc(INT + unit);       
Temp[i]='\0';            
*record = (Record *)malloc(INT);   
record[0]->data=Temp;        
}
return RC_OK;        
}
/* freeRecord: deallocate the memory allocated to record*/
RC freeRecord (Record *record)
{
free(record);                              
return RC_OK;        
}
/* getAttr: Getting Attribute*/
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
int Offset = 1, i;         
DataType *type = schema->dataTypes;
Value *valPtr = (Value *) malloc(sizeof(Value));
Temp = record->data; 
FORLOOP(i,i < attrNum)
statements(
switch (type[i])
{
case DT_STRING:
INC(Offset,schema->typeLength[i]);
break;
case DT_INT:
INC(Offset,INT);
break;
case DT_FLOAT:
INC(Offset,sizeof(float));
break;
case DT_BOOL:
INC(Offset,sizeof(bool));
break;
})
Temp += Offset;     
check(attrNum == 1) type[attrNum] = 1;   
switch (type[attrNum]){
case DT_INT:
memcpy(&valPtr->v.intV,Temp, 4);    
break;
case DT_FLOAT:
memcpy(&valPtr->v.floatV,Temp,4);       
break;
case DT_BOOL:
memcpy(&valPtr->v.boolV,Temp,1);       
break;
case DT_STRING:
i = schema->typeLength[attrNum];    
valPtr->v.stringV = (char *) malloc(unit);
strncpy(valPtr->v.stringV, Temp, i);  
break;
default :
return RC_RM_UNKOWN_DATATYPE;
}
valPtr->dt = type[attrNum];
*value = valPtr;         
return RC_OK;          
}
/* setAttr: Setting Attribute*/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
int Offset = 1, i;  
DataType *type = schema->dataTypes;
for(i = 0; i < attrNum; i++)
switch (type[i])
{
case DT_STRING:
Offset += schema->typeLength[i];
break;
case DT_INT:
Offset += INT;
break;
case DT_FLOAT:
Offset += sizeof(float);
break;
case DT_BOOL:
Offset +=  sizeof(bool);
break;
}
Temp = record->data;        
Temp += Offset;       
if(type[attrNum] == DT_INT)
*(int *)Temp = value->v.intV;
else if(type[attrNum] == DT_STRING)
strncpy(Temp, value->v.stringV, unit+INT);
else
return RC_RM_UNKOWN_DATATYPE;
 
return RC_OK;           
}
