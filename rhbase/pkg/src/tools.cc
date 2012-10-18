/**
 * Copyright 2011 Revolution Analytics
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <iostream>
#include <unistd.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
#include "Hbase.h"

using namespace apache::hadoop::hbase::thrift;

#include "rhbase.h"

extern "C"{
  SEXP initialize(SEXP host, SEXP portandbuff, SEXP transporttype){
    boost::shared_ptr<TTransport> socket(new TSocket(std::string(CHAR(STRING_ELT(host,0))), INTEGER(portandbuff)[0]));

	boost::shared_ptr<TTransport> transport;
	if (INTEGER(transporttype)[0] == 0) {
		boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket));
		transport = transport2;
	}
	else {
		boost::shared_ptr<TTransport> transport2(new TFramedTransport(socket));
		transport = transport2;
	}
	   

	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    HbaseClient *client = new HbaseClient(protocol);
    SEXP result = R_NilValue;
    try{
      transport->open();
      PROTECT(result = R_MakeExternalPtr((void*)client, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } catch (TException &tx) {
      Rf_error("rhbase:: %s",tx.what());
    }
    return(result);
  }

  SEXP deleteclient(SEXP r){
    HbaseClient *ch  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    delete ch;
    return(R_NilValue);
  }
  SEXP hb_get_tables(SEXP r){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    std::vector<std::string> tables;
    SEXP result = R_NilValue;
    try{
      client->getTableNames(tables);
      if(tables.size()>0){
	PROTECT(result = Rf_allocVector(STRSXP,tables.size()));
	for(unsigned int i=0;i < tables.size(); i++){
	  SET_STRING_ELT(result,i,Rf_mkChar(static_cast<const char*>(tables[i].c_str())));
	}
	UNPROTECT(1);
      }
    } 
    catch( IOError &tx) {
      Rf_error("rhbase:: (IOError) %s",tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase:: %s",tx.what());
    }
    return(result);
  }


  SEXP hbIsTableEnabled(SEXP r,SEXP tb){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    SEXP result = R_NilValue;
    try{
      if(client->isTableEnabled(std::string(tbn))){
	PROTECT(result = Rf_allocVector(LGLSXP,1));
	LOGICAL(result)[0] = 1;
	UNPROTECT(1);
      }else{
	PROTECT(result = Rf_allocVector(LGLSXP,1));
	LOGICAL(result)[0] = 0;
	UNPROTECT(1);
      }
    }catch( IOError &tx) {
      Rf_error("rhbase:: (IOError) %s",tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase:: %s",tx.what());
    }
    return(result);
  }

  SEXP hbTableMode(SEXP r,SEXP tb,SEXP state){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    int st = INTEGER(state)[0];
    SEXP result = R_NilValue;
    std::string tbsn(tbn);
    try{
      if(st>0) client->enableTable(tbsn); else client->disableTable(tbsn);
      if(client->isTableEnabled(tbsn)){
	PROTECT(result = Rf_allocVector(LGLSXP,1));
	LOGICAL(result)[0] = 1;
	UNPROTECT(1);
      }else{
	PROTECT(result = Rf_allocVector(LGLSXP,1));
	LOGICAL(result)[0] = 0;
	UNPROTECT(1);
      }
    }catch( IOError &tx) {
      Rf_error("rhbase<hbTableMode>:: (IOError) %s",tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbas<hbTableMode>e:: %s",tx.what());
    }
    return(result);
  }

  SEXP hbDeleteTable(SEXP r, SEXP tb){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    try{
      client->deleteTable(std::string(tbn));
    }catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  SEXP hbCompact(SEXP r,SEXP tb, SEXP mjmn){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    int mjm = INTEGER(mjmn)[0];
    try{
      if(mjm){
	// major compact
	client->majorCompact(std::string(tbn));
      } else {
	client->compact(std::string(tbn));
      }
    }catch( TException &tx) {
      Rf_error("rhbase:: %s",tx.what());
    }
    return(R_NilValue);
  }
  

  SEXP hbDescribeCols(SEXP r,SEXP tb){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    SEXP result = R_NilValue;
    SEXP nms;
    SEXP description;
    try{
      std::map<std::string,ColumnDescriptor> cmap;
      client->getColumnDescriptors(cmap, tbn);
      if(cmap.size()>0){
	PROTECT(result = Rf_allocVector(VECSXP,cmap.size()));
	PROTECT(nms = Rf_allocVector(STRSXP,LENGTH(result)));
	int i=0;
	for (std::map<std::string,ColumnDescriptor>::const_iterator it = cmap.begin(); it != cmap.end(); ++it,i++) {
	  SET_STRING_ELT(nms, i, Rf_mkChar( it->second.name.c_str()));
	  PROTECT(description = Rf_allocVector(VECSXP,8));
	  SET_VECTOR_ELT(description,0,Rf_ScalarInteger(it->second.maxVersions));
	  SET_VECTOR_ELT(description,1,Rf_ScalarString(Rf_mkChar(it->second.compression.c_str())));
	  SET_VECTOR_ELT(description,2,Rf_ScalarLogical(it->second.inMemory));
	  SET_VECTOR_ELT(description,3,Rf_ScalarString(Rf_mkChar(it->second.bloomFilterType.c_str())));
	  SET_VECTOR_ELT(description,4,Rf_ScalarInteger(it->second.bloomFilterVectorSize));
	  SET_VECTOR_ELT(description,5,Rf_ScalarInteger(it->second.bloomFilterNbHashes));
	  SET_VECTOR_ELT(description,6,Rf_ScalarLogical(it->second.blockCacheEnabled));
	  SET_VECTOR_ELT(description,7,Rf_ScalarInteger(it->second.timeToLive));
	  SET_VECTOR_ELT(result, i, description);
	  UNPROTECT(1);
	}
	Rf_setAttrib(result,Rf_install("names"),nms);
	UNPROTECT(2);
      }
    }catch( TException &tx) {
      Rf_error("rhbase:: %s",tx.what());
    }
    return(result);
  }

  SEXP hbnewBatchMutation(SEXP i){
    std::vector<BatchMutation> *bm = new std::vector<BatchMutation>();
    SEXP result;
    PROTECT(result = R_MakeExternalPtr((void*)bm, R_NilValue,R_NilValue));
    UNPROTECT(1);
    return(result);
  }
  SEXP addToCurrentBatch(SEXP bm,SEXP r,SEXP nm, SEXP lov){
    std::vector<BatchMutation> *b = static_cast<std::vector<BatchMutation>*>(R_ExternalPtrAddr(bm));
    std::vector<Mutation> mutations;
    for(int i=0;i < LENGTH(nm);i++){
      mutations.push_back(Mutation());
      mutations.back().column = CHAR(STRING_ELT(nm, i));
      SEXP s = VECTOR_ELT(lov, i);
      mutations.back().value = std::string((const char *)RAW(s), LENGTH(s));
    }
    b->push_back(BatchMutation());
    b->back().row = std::string((const char*)RAW(r),LENGTH(r));
    b->back().mutations = mutations;
    return(R_NilValue);
  }
  SEXP addAndSendDFBatch(SEXP hbc,SEXP tbname,SEXP szr, SEXP coln,SEXP lov_){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(hbc));
    std::vector<BatchMutation> *bm = new std::vector<BatchMutation>();
    for(int j=0;j< LENGTH(szr);j++){
      std::vector<Mutation> mutations;
      SEXP lovi = VECTOR_ELT(lov_,j);
      for(int i=0;i < LENGTH(coln);i++){
	mutations.push_back(Mutation());
	mutations.back().column = CHAR(STRING_ELT(coln, i));
	SEXP s = VECTOR_ELT(lovi, i);
	mutations.back().value = std::string((const char *)RAW(s), LENGTH(s));
      }
      SEXP r = VECTOR_ELT(szr,j);
      bm->push_back(BatchMutation());
      bm->back().row = std::string((const char*)RAW(r),LENGTH(r));
      bm->back().mutations = mutations;
    }
    try{
      client->mutateRows(std::string(CHAR(STRING_ELT(tbname,0))), *bm);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  SEXP sendCurrentBatch(SEXP r, SEXP tbname,SEXP bm){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    std::vector<BatchMutation> *b = static_cast<std::vector<BatchMutation>*>(R_ExternalPtrAddr(bm));
    try{
      client->mutateRows(std::string(CHAR(STRING_ELT(tbname,0))), *b);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }
    delete b;
    return(R_NilValue);
  }
  SEXP makeAndSendOneMutation(SEXP r,SEXP tab,SEXP ro, SEXP n, SEXP lov){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    std::vector<Mutation> mutations;
    try{
      for(int i=0;i < LENGTH(n);i++){
	mutations.push_back(Mutation());
	mutations.back().column = CHAR(STRING_ELT(n,i));
	SEXP f = VECTOR_ELT(lov,i);
	mutations.back().value = std::string( (const char*)RAW(f),LENGTH(f));
	client->mutateRow(std::string(CHAR(STRING_ELT(tab,0))), std::string((const char*)RAW(ro),LENGTH(ro)), mutations);
      }
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }
  inline SEXP parseRowResult(std::vector<TRowResult> rowResult,SEXP r){
    SEXP a;
    PROTECT(a = Rf_allocVector(VECSXP,3));
    SET_VECTOR_ELT(a, 0, r);
    if(rowResult.size()>0){
      SEXP n,v;
      PROTECT(n=Rf_allocVector(STRSXP, rowResult[0].columns.size()));
      PROTECT(v=Rf_allocVector(VECSXP, rowResult[0].columns.size()));
      int count=0;
      for (std::map<std::string,TCell>::const_iterator it = rowResult[0].columns.begin(); 
	   it != rowResult[0].columns.end(); count++,++it) {
	SET_STRING_ELT(n,count,Rf_mkChar(it->first.c_str()));
	std::string p = it->second.value;
	SEXP rr;
	PROTECT(rr = Rf_allocVector(RAWSXP, p.size()));
	memcpy(RAW(rr),p.c_str(),LENGTH(rr));
	SET_VECTOR_ELT(v,count, rr);
	UNPROTECT(1); // rr
      }
      SET_VECTOR_ELT(a,1,n);
      SET_VECTOR_ELT(a,2,v);
      UNPROTECT(2) ; //n,v
    }else{
      SET_VECTOR_ELT(a,1,R_NilValue);
      SET_VECTOR_ELT(a,2,R_NilValue);
    }
    UNPROTECT(1); //a
    return(a);
  }

  SEXP hbgetRow(SEXP r, SEXP tb, SEXP rows){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    std::vector<TRowResult> rowResult;
    //this can be optimized by stashing a copy of the string for a table
    SEXP result = R_NilValue;
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    try{
      PROTECT(result = Rf_allocVector(VECSXP, LENGTH(rows)));
      for(int i=0;i < LENGTH(rows);i++){
	rowResult.clear();
	SEXP r = VECTOR_ELT(rows,i);
	client->getRow(rowResult,tbb,std::string( (const char *)RAW(r), LENGTH(r) ));
	SET_VECTOR_ELT(result,i,parseRowResult(rowResult,r));
      }
    }
    catch( IOError &tx) {
      UNPROTECT(1); //result
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      UNPROTECT(1); //result
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
    UNPROTECT(1);
    return(result);
  }
  SEXP hbgetRowWithColumns(SEXP r, SEXP tb, SEXP rows,SEXP cols){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    std::vector<TRowResult> rowResult;
    //this can be optimized by stashing a copy of the string for a table
    SEXP result = R_NilValue;
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    std::vector<string> colss;
    for(int j=0;j<LENGTH(cols);j++) colss.push_back(std::string(CHAR(STRING_ELT(cols,j))));
    try{
      PROTECT(result = Rf_allocVector(VECSXP, LENGTH(rows)));
      for(int i=0;i < LENGTH(rows);i++){
	rowResult.clear();
	SEXP r = VECTOR_ELT(rows,i);
	client->getRowWithColumns(rowResult,tbb,std::string( (const char *)RAW(r), LENGTH(r) ),colss);
	SET_VECTOR_ELT(result,i,parseRowResult(rowResult,r));
      }
    }
    catch( IOError &tx) {
      UNPROTECT(1); //result
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      UNPROTECT(1); //result
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
    UNPROTECT(1);
    return(result);
  }

  SEXP hbdeleteAllRow(SEXP r, SEXP tb, SEXP rows,SEXP allver){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    int p = (int)LOGICAL(allver)[0];
    try{
      if(p){
	for(int i=0;i < LENGTH(rows);i++){
	  SEXP r = VECTOR_ELT(rows,i);
	  client->deleteAllRow(tbb,std::string( (const char *)RAW(r), LENGTH(r) ));
	}
      }else{
	std::vector<BatchMutation> bm;
	std::vector<Mutation> mutations;
	for(int i=0;i< LENGTH(rows);i++){
	  bm.push_back(BatchMutation()); //for every row
	  SEXP r = VECTOR_ELT(rows,i);
	  mutations.push_back(Mutation()); // once for the row
	  mutations.back().isDelete=true;
	  bm.back().row = std::string((const char*)RAW(r),LENGTH(r));
	  bm.back().mutations = mutations;
	}
	client->mutateRows(tbb,bm);
      }
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  SEXP hbdeleteAll(SEXP r, SEXP tb, SEXP rows,SEXP cn,SEXP allver){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    int p = (int)LOGICAL(allver)[0];
    std::vector<string> f;
    for(int j=0;j<LENGTH(cn);j++) f.push_back(std::string(CHAR(STRING_ELT(cn,j))));
    try{
      if(p){
	for(int i=0;i < LENGTH(rows);i++){
	  SEXP r = VECTOR_ELT(rows,i);
	  std::string pu = std::string( (const char *)RAW(r), LENGTH(r) );
	  for(unsigned int j=0;j< f.size();j++)
	    client->deleteAll(tbb,pu, f[j]);
	}
      }else{
	std::vector<BatchMutation> bm;
	std::vector<Mutation> mutations;
	for(int i=0;i< LENGTH(rows);i++){
	  SEXP r = VECTOR_ELT(rows,i);
	  bm.push_back(BatchMutation());
	  bm.back().row = std::string((const char*)RAW(r),LENGTH(r));
	  for(unsigned int j=0;j<f.size();j++) {
	    mutations.push_back(Mutation());
	    mutations.back().isDelete=true;
	    mutations.back().column = f[j];
	  }
	  bm.back().mutations = mutations;
	}
	client->mutateRows(tbb,bm);
      }
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
   return(R_NilValue);
  }
  
  SEXP hbScannerOpenFilter(SEXP r, SEXP tb, SEXP st,SEXP en,SEXP cp,SEXP typ){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    std::vector<string> f;
    int t = INTEGER(typ)[0];
    SEXP result=R_NilValue;
    for(int j=0;j<LENGTH(cp);j++) f.push_back(std::string(CHAR(STRING_ELT(cp,j))));
    try{
      ScannerID s=0 ;
      if(t==0)
	// I think if the second parameter is NULL and 3rd is 0, the scanner will start from the beginning.
	// This code won't let that occur, so it might need a fix
	s=client->scannerOpenWithStop(tbb,std::string((const char*)RAW(st),LENGTH(st)), std::string((const char*)RAW(en),LENGTH(en)), f);
	  else if(t == 1) 
	s=client->scannerOpen(tbb,std::string((const char*)RAW(st),LENGTH(st)), f);
      else if(t == 2 )
	s=client->scannerOpen(tbb,std::string((const char*)RAW(st),LENGTH(st)), f);
      result=Rf_ScalarInteger((int32_t)s);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  SEXP hbScannerOpenFilterEx(SEXP r, SEXP tb, SEXP st, SEXP en, SEXP cp, SEXP ts, SEXP caching, SEXP filter){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    std::vector<string> f;
    SEXP result=R_NilValue;
    for(int j=0;j<LENGTH(cp);j++) f.push_back(std::string(CHAR(STRING_ELT(cp,j))));

	try{
		ScannerID s=0 ;
		TScan tscan;
		if (LENGTH(st) > 0)
			tscan.__set_startRow(std::string((const char*)RAW(st)));
		if (LENGTH(en) > 0)
			tscan.__set_stopRow(std::string((const char*)RAW(en)));
		if (LENGTH(cp) > 0)
			tscan.__set_columns(f);
		if (REAL(ts)[0] > 0)
			tscan.__set_timestamp((int64_t)REAL(ts)[0]);
		if (INTEGER(caching)[0] > 0)
			tscan.__set_caching(INTEGER(caching)[0]);
		if (LENGTH(filter) > 0)
			tscan.__set_filterString(std::string(CHAR(STRING_ELT(filter,0))));
		
		s=client->scannerOpenWithScan(tbb, tscan);

		result=Rf_ScalarInteger((int32_t)s);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( TApplicationException &tx) {
      Rf_error("rhbase<%s>:: (TApplicationException) %s",__FUNCTION__,tx.what());
    }
    catch( TProtocolException &tx) {
      Rf_error("rhbase<%s>:: (TProtocolException) %s",__FUNCTION__,tx.what());
    }
    catch( TTransportException &tx) {
      Rf_error("rhbase<%s>:: (TTransportException) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
	  Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  // This is ugly - code duplication
  inline SEXP parseLotsRowResult(std::vector<TRowResult> rowResult){
    SEXP a,result;
    if(rowResult.size()==0) return(R_NilValue);
    PROTECT(result = Rf_allocVector(VECSXP, rowResult.size()));
    for(unsigned int u=0;u<rowResult.size();u++){
      SEXP r;
      PROTECT(a = Rf_allocVector(VECSXP,3));
      string k = rowResult[u].row;
      PROTECT( r = Rf_allocVector(RAWSXP,k.size()));
      memcpy(RAW(r),k.c_str(),k.size());
      SET_VECTOR_ELT(a, 0, r);
      UNPROTECT(1);
      SEXP n,v;
      PROTECT(n=Rf_allocVector(STRSXP, rowResult[u].columns.size()));
      PROTECT(v=Rf_allocVector(VECSXP, rowResult[u].columns.size()));
      int count=0;
      for (std::map<std::string,TCell>::const_iterator it = rowResult[u].columns.begin(); 
	   it != rowResult[u].columns.end(); count++,++it) {
	SET_STRING_ELT(n,count,Rf_mkChar(it->first.c_str()));
	std::string p = it->second.value;
	SEXP rr;
	PROTECT(rr = Rf_allocVector(RAWSXP, p.size()));
	memcpy(RAW(rr),p.c_str(),LENGTH(rr));
	SET_VECTOR_ELT(v,count, rr);
	UNPROTECT(1); // rr
      }
      SET_VECTOR_ELT(a,1,n);
      SET_VECTOR_ELT(a,2,v);
      UNPROTECT(2) ; //n,v
      SET_VECTOR_ELT(result,u,a);
      UNPROTECT(1); //a
    }
    UNPROTECT(1); //result
    return(result);
  }

  SEXP hbScannerGetList(SEXP r, SEXP s, SEXP bz){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    ScannerID ss = (ScannerID)INTEGER(s)[0];
    std::vector<TRowResult> rowResult;
    SEXP result;
    try{
      client->scannerGetList(rowResult,ss, INTEGER(bz)[0]);
      PROTECT(result = parseLotsRowResult(rowResult));
      UNPROTECT(1);
      return(result);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( IllegalArgument &tx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }    
    return(R_NilValue);
  }


  SEXP hbCloseScanner(SEXP r,SEXP s){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    ScannerID ss = (ScannerID)INTEGER(s)[0];
    try{
      client->scannerClose(ss);
    }
    catch( IOError &tx) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,tx.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: (TException) %s",__FUNCTION__,tx.what());
    }    return(R_NilValue);
    return(R_NilValue);
  }

  inline SEXP getListElement(SEXP list, const char *str)
  {
    SEXP elmt = R_NilValue, names = getAttrib(list, R_NamesSymbol);
    int i;
    for (i = 0; i < length(list); i++)
         if(strcmp(CHAR(STRING_ELT(names, i)), str) == 0) {
           elmt = VECTOR_ELT(list, i);
           break;
         }
    return elmt;
  }

  SEXP hbCreateTable(SEXP r,SEXP tb,SEXP cd1){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    std::vector<ColumnDescriptor> cd;
    try{
      for(int i=0;i < LENGTH(cd1);i++){
	SEXP cname  = VECTOR_ELT(VECTOR_ELT(cd1,i),0);
	SEXP opts = VECTOR_ELT(VECTOR_ELT(cd1,i),1);
	cd.push_back(ColumnDescriptor());
	cd.back().name = std::string(CHAR(STRING_ELT(cname,0)));
	
	// This could have been macrofied, but then I'd have to learn it(macros)
	// and i just want to get this out of the way.
	SEXP t = getListElement(opts,"maxversions");
	if(t!=R_NilValue) cd.back().maxVersions = INTEGER(t)[0];

	t = getListElement(opts,"compression");
	if(t!=R_NilValue) cd.back().compression = std::string(CHAR(STRING_ELT(t,0)));
	
	t = getListElement(opts,"inmemory");
	if(t!=R_NilValue) cd.back().inMemory = LOGICAL(t)[0] == 1? true :false;

	t = getListElement(opts,"bloomfiltertype");
	if(t!=R_NilValue) cd.back().bloomFilterType = std::string(CHAR(STRING_ELT(t,0)));

	t = getListElement(opts,"bloomfiltervectorsize");
	if(t!=R_NilValue) cd.back().bloomFilterVectorSize = INTEGER(t)[0];

	t = getListElement(opts,"bloomfilternbhashes");
	if(t!=R_NilValue) cd.back().bloomFilterNbHashes = INTEGER(t)[0];

	t = getListElement(opts,"blockcacheenabled");
	if(t!=R_NilValue) cd.back().blockCacheEnabled = LOGICAL(t)[0] == 1? true :false;

	t = getListElement(opts,"timetolive");
	if(t!=R_NilValue) cd.back().timeToLive = INTEGER(t)[0];
      }
      client->createTable(tbb, cd);
    }
    catch( AlreadyExists &ax) {
      Rf_error("rhbase<%s>:: (AlreadyExists) %s",__FUNCTION__,ax.what());
    }
    catch( IllegalArgument &bx) {
      Rf_error("rhbase<%s>:: (IllegalArgument) %s",__FUNCTION__,bx.what());
    }
    catch( IOError &ex) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,ex.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);


  }

  SEXP hbGetRegions(SEXP r, SEXP tb){
    HbaseClient *client  = static_cast<HbaseClient*>(R_ExternalPtrAddr(r));
    string tbb=std::string(CHAR(STRING_ELT(tb,0)));
    SEXP result  = R_NilValue;
    try{
      std::vector<TRegionInfo> rg;
      client->getTableRegions(rg,tbb);
      if(rg.size() == 0) return(R_NilValue);
      PROTECT(result = Rf_allocVector(VECSXP, rg.size()));
      for(unsigned int i=0;i < rg.size();i++){
	TRegionInfo t = rg[i];
	SEXP inner;
	PROTECT(inner = Rf_allocVector(VECSXP, 5));
	SET_VECTOR_ELT(inner,4, Rf_ScalarRaw(t.version));

	SEXP f;
	PROTECT(f = Rf_allocVector(RAWSXP, t.name.size()));
	memcpy(RAW(f), t.name.c_str(),LENGTH(f));
	SET_VECTOR_ELT(inner,3, f);
	UNPROTECT(1);

	SET_VECTOR_ELT(inner,2, Rf_ScalarReal((double)t.id));

	std::string ss = t.startKey, se=t.endKey;
	if(se.size()>0){
	  SEXP e;
	  PROTECT(e = Rf_allocVector(RAWSXP, se.size()));
	  memcpy(RAW(e),se.c_str(),LENGTH(e));
	  SET_VECTOR_ELT(inner,1, e);
	  UNPROTECT(1);
	}else SET_VECTOR_ELT(inner,1,R_NilValue);

	if(ss.size()>0){
	  SEXP s;
	  PROTECT(s = Rf_allocVector(RAWSXP, ss.size()));
	  memcpy(RAW(s),ss.c_str(),LENGTH(s));
	  SET_VECTOR_ELT(inner,0, s);
	  UNPROTECT(1);
	} else SET_VECTOR_ELT(inner,0,R_NilValue);
	
	SET_VECTOR_ELT(result,i, inner);
	UNPROTECT(1); // inner
      }
      UNPROTECT(1);
      return(result);
    }
    catch( IOError &ex) {
      Rf_error("rhbase<%s>:: (IOError) %s",__FUNCTION__,ex.what());
    }
    catch( TException &tx) {
      Rf_error("rhbase<%s>:: %s",__FUNCTION__,tx.what());
    }


    return(result);
  }
} //End extern "C"

