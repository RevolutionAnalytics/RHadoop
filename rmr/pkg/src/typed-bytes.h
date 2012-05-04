//Copyright 2011 Revolution Analytics
//   
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS, 
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

#ifndef _RMR_TYPEDBYTES_H
#define _RMR_TYPEDBYTES_H

#include <Rcpp.h>


RcppExport SEXP typed_bytes_reader(SEXP data, SEXP nobjs);
RcppExport SEXP typed_bytes_writer(SEXP data);

#endif
