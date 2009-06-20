

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
//#include "../config.h"
#endif
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"

#include "../Db/dbtest.h"



int filldb(void *db, int databasesize, int recordsize){

	int i, j, tmp;
	void *rec;
	wg_int value = 0;
	int increment = 1;
	int incrementincrement = 17;
	int k = 0;
	for (i=0;i<databasesize;i++) {
		rec=wg_create_record(db,recordsize);
		if (rec==NULL) { 
			printf("rec creation error\n");
			continue;
		}
		
		for(j=0;j<recordsize;j++){
			tmp=wg_set_int_field(db,rec,j,value+j);
			if (tmp!=0) { 
				printf("int storage error\n");   
			}
		}
		value += increment;
		if(k % 2 == 0)increment += 2;
		else increment -= 1;
		k++;
		if(k == incrementincrement) {increment = 1; k = 0;}
	} 

	return 0;
}

int filldb2(void *db, int databasesize, int recordsize){

	int i, j, tmp;
	void *rec;
	wg_int value = 100000;
	int increment = -1;
	int incrementincrement = 17;
	int k = 0;
	for (i=0;i<databasesize;i++) {
		rec=wg_create_record(db,recordsize);
		if (rec==NULL) { 
			printf("rec creation error\n");
			continue;
		}
		
		for(j=0;j<recordsize;j++){
			tmp=wg_set_int_field(db,rec,j,value+j);
			if (tmp!=0) { 
				printf("int storage error\n");   
			}
		}
		value += increment;
		if(k % 2 == 0)increment -= 2;
		else increment += 1;
		k++;
		if(k == incrementincrement) {increment = -1; k = 0;}
	} 

	return 0;
}


int filldb3(void *db, int databasesize, int recordsize){

	int i, j, tmp;
	void *rec;
	wg_int value = 1;
	int increment = 1;
	int incrementincrement = 18;
	int k = 0;
	for (i=0;i<databasesize;i++) {
		rec=wg_create_record(db,recordsize);
		if (rec==NULL) { 
			printf("rec creation error\n");
			continue;
		}
		if(k % 2)value = 10000 - value;
		for(j=0;j<recordsize;j++){
			tmp=wg_set_int_field(db,rec,j,value+j);
			if (tmp!=0) { 
				printf("int storage error\n");   
			}
		}
		if(k % 2)value = 10000 - value;
		value += increment;
		

		if(k % 2 == 0)increment += 2;
		else increment -= 1;
		k++;
		if(k == incrementincrement) {increment = 1; k = 0;}
	} 

	return 0;
}

int selectdata(void *db, int howmany, int startingat){

	void *rec = wg_get_first_record(db);

	int i;
	for(i=0;i<startingat;i++){
		if(rec == NULL) return 0;
		rec=wg_get_next_record(db,rec); 
	}

	int fields;
	wg_int encoded;
	int count=0;
	while(rec != NULL) {
		fields = wg_get_record_len(db, rec);
		for(i=0;i<fields;i++){
			encoded = wg_get_field(db, rec, i);
			printf("%7d\t",wg_decode_int(db,encoded));
		}
		printf("\n");
		count++;
		if(count == howmany)break;
		rec=wg_get_next_record(db,rec);
	}

	return 0;
}

int findslow(void *db, int column, int key){

	void *rec = wg_get_first_record(db);

	int fields;
	wg_int encoded;
	int i;
	while(rec != NULL) {
		encoded = wg_get_field(db, rec, column);
		if(wg_decode_int(db,encoded) == key){
			fields = wg_get_record_len(db, rec);
			printf("data row found: ");
			for(i=0;i<fields;i++){
				encoded = wg_get_field(db, rec, i);
				printf("%7d\t",wg_decode_int(db,encoded));
			}
			printf("\n");
			return 0;
		}
		
		rec=wg_get_next_record(db,rec);
	}
	printf("key %d not found\n",key);
	return 0;
}

static int printhelp(){
	printf("\nindextool user commands:\n");
	printf("indextool <base name> fill <nr of rows> [asc | desc | mix] - fill db\n");
	printf("indextool <base name> createindex <column> - create ttree index\n");
	printf("indextool <base name> select  <number of rows> <start from row> - print db contents\n");
	printf("indextool <base name> logtree <column> [filename] - log tree\n");
	printf("indextool <base name> header - print header data\n");
	printf("indextool <base name> fast <column> <key> - search data from index\n");
	printf("indextool <base name> slow <column> <key> - search data with sequencial scan\n");
	printf("indextool <base name> add <value1> <value2> <value3> - store data row and make an index entries\n");
	printf("indextool <base name> del <column> <key> - delete data row and its index entries\n");
	printf("indextool <base name> free - free db\n\n");
	return 0;
}


int main(int argc, char **argv) {
 
	char* shmname;
	char* shmptr;
	char* command;
	int tmp;
	void *rec;
	int i,j;
	unsigned int databasesize = 60;
	int recordsize = 3;
	
	if(argc < 3){
		printhelp();
		return 0;
	}

	command = argv[2];
	shmname = argv[1];
	
	

	shmptr=wg_attach_database(shmname,10000000); // 0 size causes default size to be used
	void *db = shmptr;
	printf("wg_attach_database on %d gave ptr %x\n",DEFAULT_MEMDBASE_KEY,(int)shmptr);
	if(shmptr==NULL){
		printf("wg_attach_database on %d gave fatal error\n",DEFAULT_MEMDBASE_KEY);
		return 0;
	}

	if(strcmp(command,"free")==0) {
		// free shared memory and exit
		wg_delete_database(shmname);
		return 0;    
	}

	if(strcmp(command,"header")==0) {
		show_db_memsegment_header(shmptr);
		return 0;    
	}

	if(strcmp(command,"fill")==0) {
		if(argc < 4){printhelp();return 0;}
		int n;
		sscanf(argv[3],"%d",&n);
		if(argc > 4 && strcmp(argv[4],"mix")==0) filldb3(db, n, recordsize);
		else if(argc > 4 && strcmp(argv[4],"desc")==0)filldb2(db, n, recordsize);
		else filldb(db, n, recordsize);
		printf("data inserted\n");
		return 0;    
	} 

	if(strcmp(command,"select")==0) {
		if(argc < 5){printhelp();return 0;}
		int s,c;
		sscanf(argv[3],"%d",&s);
		sscanf(argv[4],"%d",&c);
		selectdata(db,s,c);
		return 0;    
	}

	if(strcmp(command,"createindex")==0) {
		if(argc < 4){printhelp();return 0;}
		int col;
		sscanf(argv[3],"%d",&col);
		db_create_ttree_index(db, col);
		return 0;    
	}

	if(strcmp(command,"logtree")==0) {
		if(argc < 4){printhelp();return 0;}
		int col;
		sscanf(argv[3],"%d",&col);
		char *a = "tree.xml";
		if(argc > 4)a = argv[4];
		db_memsegment_header* dbh;
		dbh = (db_memsegment_header*) db;
		int i = db_column_to_index_id(db, col);
		if(i!=-1)log_tree(db,a,offsettoptr(db,dbh->index_control_area_header.index_array[i].offset_root_node));
		return 0;    
	}
		
	if(strcmp(command,"slow")==0) {
		if(argc < 5){printhelp();return 0;}
		int c,k;
		sscanf(argv[3],"%d",&c);
		sscanf(argv[4],"%d",&k);
		findslow(db,c,k);
		return 0;    
	}	
	if(strcmp(command,"fast")==0) {
		if(argc < 5){printhelp();return 0;}
		int c,k;
		sscanf(argv[3],"%d",&c);
		sscanf(argv[4],"%d",&k);
		int i = db_column_to_index_id(db, c);
		if(i!=-1){
			wg_int encoded;
			wg_int offset = db_search_ttree_index(db, i, k);
			if(offset != 0){
				void *rec = offsettoptr(db,offset);
				int fields = wg_get_record_len(db, rec);
				for(i=0;i<fields;i++){
					encoded = wg_get_field(db, rec, i);
					printf("%7d\t",wg_decode_int(db,encoded));
				}
				printf("\n");
			}else{
				printf("cannot find key %d in index\n",k);
			}
			
		}
		return 0;    
	}

	if(strcmp(command,"add")==0) {
		if(argc < 6){printhelp();return 0;}
		int a,b,c;
		a=b=c=0;
		sscanf(argv[3],"%d",&a);
		sscanf(argv[4],"%d",&b);
		sscanf(argv[5],"%d",&c);
		void *rec = wg_create_record(db,3);
		if (rec==NULL) { 
			printf("rec creation error\n");
			return 1;
		}else{
			wg_set_int_field(db,rec,0,a);
			wg_set_int_field(db,rec,1,b);
			wg_set_int_field(db,rec,2,c);
			int i;
			int col;
			db_memsegment_header* dbh;
			dbh = (db_memsegment_header*) db;
			for(i=0;i<maxnumberofindexes;i++){
				if(dbh->index_control_area_header.index_array[i].offset_root_node!=0){
					col = dbh->index_control_area_header.index_array[i].rec_field_index;
					if(col == 0 || col == 1 || col == 2){
						db_add_new_row_into_index(db, i, rec);
					}
				}
			}
		}
		return 0;    
	}
	
	if(strcmp(command,"del")==0) {
		if(argc < 5){printhelp();return 0;}
		int c,k;
		
		sscanf(argv[3],"%d",&c);
		sscanf(argv[4],"%d",&k);
		
		void *rec = NULL;
		int i;
		int col;
		db_memsegment_header* dbh;
		dbh = (db_memsegment_header*) db;

		i = db_column_to_index_id(db, c);
		if(i!=-1){
			wg_int encoded;
			wg_int offset = db_search_ttree_index(db, i, k);
			if(offset != 0){
				rec = offsettoptr(db,offset);
			}
		}
		if(rec==NULL){
			printf("no such data\n");
			return 0;
		}
		int aa,bb,cc;
		

		wg_int encoded = wg_get_field(db, rec, 0);
		aa = wg_decode_int(db,encoded);
		encoded = wg_get_field(db, rec, 1);
		bb = wg_decode_int(db,encoded);
		encoded = wg_get_field(db, rec, 2);
		cc = wg_decode_int(db,encoded);

		for(i=0;i<maxnumberofindexes;i++){
			if(dbh->index_control_area_header.index_array[i].offset_root_node!=0){
				int asd = dbh->index_control_area_header.index_array[i].rec_field_index;
				if(asd == 0)db_remove_key_from_index(db, i, aa);
				if(asd == 1)db_remove_key_from_index(db, i, bb);
				if(asd == 2)db_remove_key_from_index(db, i, cc);
			}
		}
		printf("deleted data from indexes, but no function for deleting the record\n");//wg_delete_record(db,rec);
		return 0;    
	}


	printhelp();
	//wg_detach_database(shmptr);   
	//wg_delete_database(shmname);
	
	#ifdef _WIN32  
	_getch();  
	#endif  
	return 0;  
}


