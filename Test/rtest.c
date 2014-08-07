/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
*
* Contact: tanel.tammet@gmail.com                 
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

 /** @file rtest.c
 *  Reasoner testing functions.
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
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
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"
#include "../Db/dbdump.h"
#include "../Db/dblog.h"
#include "../Db/dbquery.h"
#include "../Db/dbutil.h"
#include "../Parser/dbparse.h"
#include "../Reasoner/rincludes.h"
#include "../Reasoner/rmain.h"
#include "rtest.h"


/* ====== Private headers and defs ======== */


/* ======= Private protos ================ */

static int wr_test_reasoner_otterparser(glb* g,int p);
static int wr_test_coretests(glb* g,int p);
static int wr_test_eq(glb* g, cvec clvec, char* clstr, int expres, int p);
static int wr_test_unify(glb* g, cvec clvec, char* clstr, int expres, int p);
static int wr_test_match(glb* g, cvec clvec, char* clstr, int expres, int p);
static int wr_test_unify(glb* g, cvec clvec, char* clstr, int expres, int p);
static int wr_litinf_is_clear(glb* g,vec v);

/* ====== Functions ============== */

/** Run reasoner tests.
 * Allows each test to be run in separate locally allocated databases,
 * if necessary.
 *
 * returns 0 if no errors.
 * otherwise returns error code.
*/


int wg_test_reasoner(int argc, char **argv) {
  void* db=NULL;
  glb* g;
  int tmp=0;
  int p=2;
  int localflag=1;
  
  printf("******** wg_test_reasoner starts ********* \n");
  if (localflag) db=wg_attach_local_database(500000);  
  if (db==NULL) {
    if (p) printf("failed to initialize database\n");
    return 1;    
  }  
  g=wr_glb_new_full(db);
  if (g==NULL) {
    if (p) printf("failed to initialize reasoner globals\n");
    return 1;    
  }
  if (tmp==0) tmp=wr_test_reasoner_otterparser(g,p); 
  if (tmp) {
    if (p) printf("failed to parse otter text\n");
    return 1;    
  }
  tmp=wr_test_coretests(g,p);
  
  //wr_init_active_passive_lists_std(g); 
  //res=wr_genloop(g);
  //printf("\nresult %d\n",res);
  //printf("----------------------------------\n");
  //wr_show_stats(g);
  //printf("----------------------------------\n");
  
  wr_glb_free(g);  
  if (!tmp) {
    if (p) printf("******** wg_test_reasoner ends OK ********\n"); 
  } else {
    if (p) printf("******** wg_test_reasoner ends with an error ********\n");
  } 
  if (localflag) wg_delete_local_database(db);
  return tmp;  
}  

static int wr_test_reasoner_otterparser(glb* g,int p) {
  int err;
  int res=0;
  char* otterstr;
  int ottestrlen;
  
  
  otterstr="p(3).  -p(?X) | =(?X,2).";
  ottestrlen=strlen(otterstr);
  
  if (p>0) printf("--------- wr_test_reasoner_otterparser starts ---------\n");
  //err = wr_import_otter_file(g,"otter.txt",otterstr,ottestrlen);  
  err = wr_import_otter_file(g,"Rexamples/otter.txt",NULL,NULL);
  if(!err) {
    if (p>1) printf("Data imported from otter file OK\n");
    res=0;
  } else if(err<-1) {
    if (p>0) printf("Fatal error when importing otter file, data may be partially imported\n");
    res=1;
  } else {
    if (p>0) printf("Import failed.\n");
    res=1;
  }
  if (p>0) printf("--------- wr_test_reasoner_otterparser ends ---------\n"); 
  return res;
}

static int wr_test_coretests(glb* g,int p) {
  int tmp=0;
  cvec clvec;
  
  if (p>0) printf("--------- wr_test_reasoner_coretests starts ---------\n");
  clvec=wr_cvec_new(g,1000);
  if (clvec==NULL) {
    printf("cannot allocate cvec in wr_test_coretests\n");
    return 1;
  }  
  
  (g->print_initial_parser_result)=0;
  (g->print_generic_parser_result)=0;
  
  if (p>0)  printf("- - - wr_equal_term tests - - -\n");
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1). m(1).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1). ?X(1).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1). 'a'(1).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,-1,0,2,100000). p(1,-1,0,2,100000).",1,p); 
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,10). p(1,20).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1.23456). p(1.23456).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1.0). p(1.0).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1.0,-10.2,10000.00001). p(1.0,-10.2,10000.00001).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1.0,-10.2,10000.00001). p(1,-10.2,10000.00001).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1'). p(a,'a1a1a1a1a1a1a1a1a1a1').",1,p); 
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1'). p(a,'a1a1a1a1a1a1a1a1a1a2').",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"abc\"). p(\"abc\").",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"abc\",\"x\"). p(\"abc\",\"x\").",1,p);
  //if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"abc\",\"\",\"x\"). p(\"abc\",\"\",\"x\").",1,p);
  //if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"abc\",\"\",\"x\"). p(\"abc\",\"\",\"y\").",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"abc\",\"xxc\"). p(\"abc\",\"xxC\").",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"0123456789012345678901234567890123456789\").\
  p(\"0123456789012345678901234567890123456789\").",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"0123456789012345678901234567890123456789\").\
  p(\"012345678901234567890123456789012345678\").",0,p); 
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"01234567890123456789012345678901234567a9\").\
  p(\"01234567890123456789012345678901234567b9\").",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,bbbb,c). p(a,bbbb,c).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,bbbb,c). p(a,bbbd,c).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,bbbb). p(a,\"bbbb\").",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,bbbb). p(a,'bbbb').",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(a,'bbbb'). p(a,\"bbbb\").",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(?X,10). p(?X,10).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(?X,10). p(X,10).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(?X,?Y). p(?X,?Y).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(?X,?X). p(?X,?Y).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,2,c),g(1))). p(1,f(a,f(1,2,c),g(1))).",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,2,c),g(1))). p(1,f(a,f(1,3,c),g(1))).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,2,c),g(1))). p(1,f(a,f(1,?X,c),g(1))).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,2,c),g(1))). p(1,f(a,D,g(1))).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,b,c),g(1))). p(1,f(a,f(1,\"b\",c),g(1))).",0,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(1,f(a,f(1,\"b\",c),g(1))). p(1,f(a,f(1,\"b\",c),g(1))).",1,p);
  if (p>0)  printf("- - - wr_unify_term const case tests - - -\n");
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1,-1,0,2,100000) | p(1,-1,0,2,100000).",1,p); 
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1,10) | p(1,20).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1.23456) | p(1.23456).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1.0) | p(1.0).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1.0,-10.2,10000.00001) | p(1.0,-10.2,10000.00001).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(1.0,-10.2,10000.00001) | p(1,-10.2,10000.00001).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1') | p(a,'a1a1a1a1a1a1a1a1a1a1').",1,p); 
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1') | p(a,'a1a1a1a1a1a1a1a1a1a2').",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(\"abc\") | p(\"abc\").",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(\"abc\",\"x\") | p(\"abc\",\"x\").",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,bbbb,c) | p(a,bbbb,c).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,bbbb,c) | p(a,bbbd,c).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(\"0123456789012345678901234567890123456789\") |\
  p(\"0123456789012345678901234567890123456789\").",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"0123456789012345678901234567890123456789\") |\
  p(\"012345678901234567890123456789012345678\").",0,p); 
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"01234567890123456789012345678901234567a9\") |\
  p(\"01234567890123456789012345678901234567b9\").",0,p);
  if (p>0)  printf("- - - wr_unify_term var case tests - - -\n");
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,10) | p(?X,10).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,10) | p(X,10).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?Y) | p(a,b).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,b) | p(?X,?Y).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(a,b).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,b) | p(?X,?X).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(?X,?X).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(?X,?Y).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X) | p(f(f(f(?X)))).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(f(f(f(?X)))) | p(?X).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X) | p(f(f(f(?Y)))).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(?X,f(f(f(?X)))).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(?X,f(f(f(?Y)))).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X) | p(?Y,f(f(f(?Y)))).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,b,?X,?Y) | p(?U,?V,?U,?V).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,b,?X,?Y) | p(?U,?V,?U,?U).",1,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(a,b,?X,?X) | p(?U,?V,?U,?V).",0,p);
  if (!tmp) tmp=wr_test_unify(g,clvec,"p(?X,?X,f(?B)) | p(?A,?B,?A).",0,p);
  if (p>0)  printf("- - - wr_match_term const case tests - - -\n");
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1,-1,0,2,100000) | p(1,-1,0,2,100000).",1,p); 
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1,10) | p(1,20).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1.23456) | p(1.23456).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1.0) | p(1.0).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1.0,-10.2,10000.00001) | p(1.0,-10.2,10000.00001).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(1.0,-10.2,10000.00001) | p(1,-10.2,10000.00001).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1') | p(a,'a1a1a1a1a1a1a1a1a1a1').",1,p); 
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,'a1a1a1a1a1a1a1a1a1a1') | p(a,'a1a1a1a1a1a1a1a1a1a2').",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(\"abc\") | p(\"abc\").",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(\"abc\",\"x\") | p(\"abc\",\"x\").",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,bbbb,c) | p(a,bbbb,c).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,bbbb,c) | p(a,bbbd,c).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(\"0123456789012345678901234567890123456789\") |\
  p(\"0123456789012345678901234567890123456789\").",1,p);
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"0123456789012345678901234567890123456789\") |\
  p(\"012345678901234567890123456789012345678\").",0,p); 
  if (!tmp) tmp=wr_test_eq(g,clvec,"p(\"01234567890123456789012345678901234567a9\") |\
  p(\"01234567890123456789012345678901234567b9\").",0,p);
  if (p>0)  printf("- - - wr_match_term var case tests - - -\n");
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,10) | p(?X,10).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,10) | p(X,10).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?Y) | p(a,b).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,b) | p(?X,?Y).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(a,b).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,b) | p(?X,?X).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(?X,?X).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(?X,?Y).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X) | p(f(f(f(?X)))).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(f(f(f(?X)))) | p(?X).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X) | p(f(f(f(?Y)))).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(?X,f(f(f(?X)))).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(?X,f(f(f(?Y)))).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X) | p(?Y,f(f(f(?Y)))).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,b,?X,?Y) | p(?U,?V,?U,?V).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?U,?V,?U,?V) | p(a,b,?X,?Y).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?U,?V,?U,?V) | p(a,?X,a,?X).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(a,b,?X,?Y) | p(?U,?V,?U,?U).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?X,?X,f(?B)) | p(?A,?B,?A).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?A,?B,?A) | p(?X,?X,f(?B)).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(g(?A,?B,?A)) | p(g(?X,?X,f(?B))).",0,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(?A,?B,?A) | p(f(?A),?X,f(?A)).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(g(?A,?B,?A)) | p(g(f(?A),?X,f(?A))).",1,p);
  if (!tmp) tmp=wr_test_match(g,clvec,"p(f(?X),?X) | p(f(?X),?Y).",0,p);
  if (p>0)  printf("- - - wr_subsume_cl tests - - -\n");
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(1). p(1).",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X). p(?X).",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X). p(1).",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(1). p(?X).",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(1). p(?X) | p(1).",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(1). p(1). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(1). p(?X) | p(1). ",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(1). p(1) | p(?X). ",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(1). p(?X) | p(2). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?Y). p(?X) | p(?X). ",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X). p(a) | m(b). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X). p(a) | p(b). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X). p(?X) | m(?Y). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X). p(?X) | p(?Y). ",0,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X). p(a) | p(b) | p(b). ",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X) | p(?Y). p(c) | p(a) | p(b) | p(b). ",1,p);
  if (!tmp) tmp=wr_test_subsume_cl(g,clvec,"p(?X) | p(?X) | p(?Y). p(c) | p(?X) | p(?Y) | p(?Z). ",0,p);
  if (p>0) printf("--------- wr_test_reasoner_coretests ends ---------\n");  
  free(clvec);
  return tmp;
}  

static int wr_test_eq(glb* g, cvec clvec, char* clstr, int expres, int p) {
  int res,tmp;
  gint t1,t2;
  
  if (p>1) printf("eq testing %s expected %d ",clstr,expres);
  tmp=wr_import_otter_file(g,NULL,clstr,clvec);
  if (tmp) { 
    if (p>0) printf("\neq testing %s: otter import failed\n",clstr);
    return 1;
  }  
  t1=rpto(g,clvec[2]);
  t2=rpto(g,clvec[3]);
  res=1;
  if (wr_equal_term(g,t1,t2,1)) {
    if (expres) res=0;
  } else {
    if (!expres) res=0;
  }    
  if (p>1) { 
    if (res) printf("test FAILED\n");
    else printf("test OK\n"); 
  }  
  return res;
}  

static int wr_test_unify(glb* g, cvec clvec, char* clstr, int expres, int p) {
  //void* db=g->db;
  int res,tmp;
  gptr cl;
  gint t1,t2;
  
  if (p>1) printf("unify testing %s expected %d ",clstr,expres);
  tmp=wr_import_otter_file(g,NULL,clstr,clvec);
  if (tmp) { 
    if (p>0) printf("\nunify testing %s: otter import failed\n",clstr);
    return 1;
  }  
  cl=(gptr)(clvec[2]);
  t1=wg_get_rule_clause_atom(db,cl,0);
  t2=wg_get_rule_clause_atom(db,cl,1);  
  res=1;
  wr_clear_all_varbanks(g);  
  tmp=wr_unify_term(g,t1,t2,1);
  wr_clear_varstack(g,g->varstack);
  if (!wr_varbanks_are_clear(g,g->varbanks)) {
    if (p>1) { 
      printf(" varbanks NOT CLEARED, test FAILED\n");      
    }  
    return 1;
  }    
  if (tmp) {
    if (expres) res=0;
  } else {
    if (!expres) res=0;
  }    
  if (p>1) { 
    if (res) printf("test FAILED\n");
    else printf("test OK\n"); 
  }    
  return res;
}

static int wr_test_match(glb* g, cvec clvec, char* clstr, int expres, int p) {
  //void* db=g->db;
  int res,tmp;
  gptr cl;
  gint t1,t2;
  
  if (p>1) printf("match testing %s expected %d ",clstr,expres);
  tmp=wr_import_otter_file(g,NULL,clstr,clvec);
  if (tmp) { 
    if (p>0) printf("\nmatch testing %s: otter import failed\n",clstr);
    return 1;
  }  
  cl=(gptr)(clvec[2]);
  t1=wg_get_rule_clause_atom(db,cl,0);
  t2=wg_get_rule_clause_atom(db,cl,1);  
  res=1;
  wr_clear_all_varbanks(g);  
  tmp=wr_match_term(g,t1,t2,1);
  wr_clear_varstack(g,g->varstack);
  if (!wr_varbanks_are_clear(g,g->varbanks)) {
    if (p>1) { 
      printf(" varbanks NOT CLEARED, test FAILED\n");      
    }  
    return 1;
  }    
  if (tmp) {
    if (expres) res=0;
  } else {
    if (!expres) res=0;
  }    
  if (p>1) { 
    if (res) printf("test FAILED\n");
    else printf("test OK\n"); 
  }    
  return res;
}


static int wr_test_subsume_cl(glb* g, cvec clvec, char* clstr, int expres, int p) {
  //void* db=g->db;
  int res,tmp;
  gptr cl1,cl2;
  int i2;
  
  if (p>1) printf("subsume_cl testing %s expected %d ",clstr,expres);
  tmp=wr_import_otter_file(g,NULL,clstr,clvec);
  if (tmp) { 
    if (p>0) printf("\nunify testing %s: otter import failed\n",clstr);
    return 1;
  }  
  // cl order is reversed!!
  cl1=(gptr)(clvec[3]);
  cl2=(gptr)(clvec[2]);
  res=1;
  wr_clear_all_varbanks(g);  
  for(i2=1;i2<=(g->tmp_litinf_vec)[0];i2++) (g->tmp_litinf_vec)[i2]=0;
  tmp=wr_subsume_cl(g,cl1,cl2,1);
  //wr_clear_varstack(g,g->varstack);
  if (!wr_varbanks_are_clear(g,g->varbanks)) {
    if (p>1) { 
      printf(" varbanks NOT CLEARED, test FAILED\n");      
    }  
    return 1;
  } 
  // no need for g->tmp_litinf_vec to be clear   
  //if (!wr_litinf_is_clear(g,g->tmp_litinf_vec)) {
  //  if (p>1) { 
  //    printf(" litinfo NOT CLEARED, test FAILED\n");      
  //  }  
  //  return 1;
  //} 
  if (tmp) {
    if (expres) res=0;
  } else {
    if (!expres) res=0;
  }    
  if (p>1) { 
    if (res) printf("test FAILED\n");
    else printf("test OK\n"); 
  }    
  return res;
}

/*
static int wr_litinf_is_clear(glb* g,vec v) {
  int i;
  
  for(i=1;i<v[0];i++) {
    if (v[i]!=0) return 0;
  }    
  return 1; 
}  
*/

#ifdef __cplusplus
}
#endif
