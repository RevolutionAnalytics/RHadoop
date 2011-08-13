# Copyright 2011 Revolution Analytics
#    
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

 
##refresh = function(f, args) {
  ##calculate output fname
  ## if out missing recalculate
  ## else
  ## get date of output
  ## get date of file input(s)
  ## if any inputs newer than output recalculate
  ## else return output fname
##}

#additional logic for revoMR
## revoMapReduce(input, output = hdfs.tempfile(), ...) {
##   if(!is.character(outtype)) {
##     outattr = attr(output, 'property')
##     if(outattr = 'tempfile') {
##       output = output()
##     }
##     else{## make mode
##       outfname = digest(args)
##       if (older(input, output)) return output
##     }
##   }
##   rhstream(input, output, ...)


           
##if output is missing or tempfile, write to tempfile, recompute
##if output is pseudomake, write to digest(args) if older than input
##if output is path, write to path, recompute



