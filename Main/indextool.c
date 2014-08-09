/*
* $Id:  $
* $Version: $
*
* Copyright (c) Enar Reilent 2009
* Copyright (c) Priit Järv 2013,2014
*
* This file is part of WhiteDB
*
* WhiteDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* WhiteDB is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file indextool.c
 *  Command line utility for index manipulation
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "../Db/dballoc.h"
#include "../Db/dbmem.h"
#include "../Db/dbindex.h"
#include "../Db/dbhash.h"
#include "../Db/dbutil.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define sscanf sscanf_s  /* XXX: This will break for string parameters */
#endif


/* ======= Private protos ================ */

void print_tree(void *db, FILE *file, struct wg_tnode *node, int col);
int log_tree(void *db, char *file, struct wg_tnode *node, int col);
void dump_hash(void *db, FILE *file, db_hash_area_header *ha);
wg_index_header *get_index_by_id(void *db, gint index_id);


/* ====== Functions ============== */

static int printhelp(){
  printf("\nindextool user commands:\n" \
      "indextool [shmname] logtree <index id> [filename] - log tree\n" \
      "indextool [shmname] dumphash <index id> - print hash table\n\n");
  return 0;
}


int main(int argc, char **argv) {

  char* shmname = NULL;
  void *db;
  int i, scan_to, shmsize;

  if(argc < 3) scan_to = argc;
  else scan_to = 3;
  shmsize = 0; /* 0 size causes default size to be used */

  /* Similar command parser as in wgdb.c */
  for(i=1; i<scan_to;) {
    if (!strcmp(argv[i],"help") || !strcmp(argv[i],"-h")) {
      printhelp();
      return 0;
    }

    else if(!strcmp(argv[i], "logtree")) {
      int index_id;
      char *a = "tree.xml";
      wg_index_header *hdr;

      if(argc < (i+2)) {
        printhelp();
        return 0;
      }
      db = (void *) wg_attach_database(shmname, shmsize);
      if(!db) {
        fprintf(stderr, "Failed to attach to database.\n");
        return 0;
      }
      sscanf(argv[i+1], "%d", &index_id);
      if(argc > (i+2)) a = argv[i+2];

      hdr = get_index_by_id(db, index_id);
      if(hdr) {
        if(hdr->type != WG_INDEX_TYPE_TTREE && \
          hdr->type != WG_INDEX_TYPE_TTREE_JSON) {
          fprintf(stderr, "Index type not supported.\n");
          return 0;
        }
        log_tree(db, a,
          (struct wg_tnode *) offsettoptr(db, TTREE_ROOT_NODE(hdr)),
          hdr->rec_field_index[0]);
      }
      else {
        fprintf(stderr, "Invalid index id.\n");
        return 0;
      }
      return 0;
    }

    else if(!strcmp(argv[i], "dumphash")) {
      int index_id;
      wg_index_header *hdr;

      if(argc < (i+1)) {
        printhelp();
        return 0;
      }
      db = (void *) wg_attach_database(shmname, shmsize);
      if(!db) {
        fprintf(stderr, "Failed to attach to database.\n");
        return 0;
      }
      sscanf(argv[i+1], "%d", &index_id);

      hdr = get_index_by_id(db, index_id);
      if(hdr) {
        if(hdr->type != WG_INDEX_TYPE_HASH && \
          hdr->type != WG_INDEX_TYPE_HASH_JSON) {
          fprintf(stderr, "Index type not supported.\n");
          return 0;
        }
        dump_hash(db, stdout, HASHIDX_ARRAYP(hdr));
      }
      else {
        fprintf(stderr, "Invalid index id.\n");
        return 0;
      }
      return 0;
    }

    shmname = argv[1]; /* assuming two loops max */
    i++;
  }

  printhelp();
  return 0;
}

void print_tree(void *db, FILE *file, struct wg_tnode *node, int col){
  int i;
  char strbuf[256];

  fprintf(file,"<node offset = \"%d\">\n", (int) ptrtooffset(db, node));
  fprintf(file,"<data_count>%d",node->number_of_elements);
  fprintf(file,"</data_count>\n");
  fprintf(file,"<left_subtree_height>%d",node->left_subtree_height);
  fprintf(file,"</left_subtree_height>\n");
  fprintf(file,"<right_subtree_height>%d",node->right_subtree_height);
  fprintf(file,"</right_subtree_height>\n");
#ifdef TTREE_CHAINED_NODES
  fprintf(file,"<successor>%d</successor>\n", (int) node->succ_offset);
  fprintf(file,"<predecessor>%d</predecessor>\n", (int) node->pred_offset);
#endif
  wg_snprint_value(db, node->current_min, strbuf, 255);
  fprintf(file,"<min_max>%s ",strbuf);
  wg_snprint_value(db, node->current_max, strbuf, 255);
  fprintf(file,"%s</min_max>\n",strbuf);
  fprintf(file,"<data>");
  for(i=0;i<node->number_of_elements;i++){
    wg_int encoded = wg_get_field(db,
      (struct wg_tnode *) offsettoptr(db,node->array_of_values[i]), col);
    wg_snprint_value(db, encoded, strbuf, 255);
    fprintf(file, "%s ", strbuf);
  }

  fprintf(file,"</data>\n");
  fprintf(file,"<left_child>\n");
  if(node->left_child_offset == 0)fprintf(file,"null");
  else{
    print_tree(db,file,
      (struct wg_tnode *) offsettoptr(db,node->left_child_offset),col);
  }
  fprintf(file,"</left_child>\n");
  fprintf(file,"<right_child>\n");
  if(node->right_child_offset == 0)fprintf(file,"null");
  else{
    print_tree(db,file,
      (struct wg_tnode *) offsettoptr(db,node->right_child_offset),col);
  }
  fprintf(file,"</right_child>\n");
  fprintf(file,"</node>\n");
}

int log_tree(void *db, char *file, struct wg_tnode *node, int col){
#ifdef _WIN32
  FILE *filee;
  fopen_s(&filee, file, "w");
#else
  FILE *filee = fopen(file,"w");
#endif
  print_tree(db,filee,node,col);
  fflush(filee);
  fclose(filee);
  return 0;
}

void dump_hash(void *db, FILE *file, db_hash_area_header *ha) {
  gint i;
  for(i=0; i<ha->arraylength; i++) {
    gint bucket = dbfetch(db, (ha->arraystart)+(sizeof(gint) * i));
    if(bucket) {
#ifdef _WIN32
      fprintf(file, "hash: %Id\n", i);
#else
      fprintf(file, "hash: %td\n", i);
#endif
      while(bucket) {
        gint j, rec_offset;
        gint length = dbfetch(db, bucket + HASHIDX_META_POS*sizeof(gint));
        unsigned char *dptr = offsettoptr(db, bucket + \
          HASHIDX_HEADER_SIZE*sizeof(gint));

        /* Hash string dump */
#ifdef _WIN32
        fprintf(file, "  offset: %Id ", bucket);
#else
        fprintf(file, "  offset: %td ", bucket);
#endif
        for(j=0; j<length; j++) {
          fprintf(file, " %02X", (unsigned int) (dptr[j]));
        }
        fprintf(file, " (");
        for(j=0; j<length; j++) {
          if(dptr[j] < 32 || dptr[j] > 126)
            fputc('.', file);
          else
            fputc(dptr[j], file);
        }
        fprintf(file, ")\n");

        /* Offset dump */
        fprintf(file, "    records:");
        rec_offset = dbfetch(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint));
        while(rec_offset) {
          gcell *rec_cell = (gcell *) offsettoptr(db, rec_offset);
#ifdef _WIN32
          fprintf(file, " %Id", rec_cell->car);
#else
          fprintf(file, " %td", rec_cell->car);
#endif
          rec_offset = rec_cell->cdr;
        }
        fprintf(file, "\n");

        bucket = dbfetch(db, bucket + HASHIDX_HASHCHAIN_POS*sizeof(gint));
      }
    }
  }
}


/* Find index by id
 *
 * helper function to validate index id-s. Checks if the
 * index is present in master list before converting the offset
 * into pointer.
 */
wg_index_header *get_index_by_id(void *db, gint index_id) {
  wg_index_header *hdr = NULL;
  db_memsegment_header* dbh = dbmemsegh(db);
  gint *ilist = &dbh->index_control_area_header.index_list;

  /* Locate the header */
  while(*ilist) {
    gcell *ilistelem = (gcell *) offsettoptr(db, *ilist);
    if(ilistelem->car == index_id) {
      hdr = (wg_index_header *) offsettoptr(db, index_id);
      break;
    }
    ilist = &ilistelem->cdr;
  }
  return hdr;
}

#ifdef __cplusplus
}
#endif
