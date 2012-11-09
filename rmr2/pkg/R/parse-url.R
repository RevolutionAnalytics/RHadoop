# Copyright 2011 Revolution Analytics
# Factored out from the httr package https://github.com/hadley/httr
# Originally under the MIT license
# Original author Hadley Wickham <h.wickham@gmail.com>
# No Copyright information found in the original.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


parse_url <- function(url) {
  
  url <- as.character(url)
  stopifnot(length(url) == 1)
  
  pull_off <- function(pattern) {
    if (!str_detect(url, pattern)) return(NULL)
    
    piece <- str_match(url, pattern)[, 2]
    url <<- str_replace(url, pattern, "")
    
    piece
  }
  
  fragment <- pull_off("#(.*)$")
  scheme <- pull_off("^([[:alpha:]+.-]+):")
  netloc <- pull_off("^//([^/]*)/?")
  
  if (!is.null(netloc)) {
    
    pieces <- str_split(netloc, "@")[[1]]
    if (length(pieces) == 1) {
      username <- NULL
      password <- NULL
      
      host <- pieces
    } else {
      user_pass <- str_split(pieces[[1]], ":")[[1]]
      username <- user_pass[1]
      password <- user_pass[2]
      
      host <- pieces[2]
    }
    
    host_pieces <- str_split(host, ":")[[1]]
    hostname <- host_pieces[1]
    port <- if (length(host_pieces) > 1) host_pieces[2]
  } else {
    port <- username <- password <- hostname <- NULL
  }
  
  query <- pull_off("\\?(.*)$")
  if (!is.null(query)) {
    query <- parse_query(query)
  }
  params <- pull_off(";(.*)$")
  
  structure(list(
    scheme = scheme, hostname = hostname, port = port, path = url,
    query = query, params = params, username = username, password = password),
            class = "url")
}
