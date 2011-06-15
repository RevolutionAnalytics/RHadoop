if ( $#argv == 0 ) then
   echo -2 "Usage:  MakeAll <operation> <packageList>"
   echo -2
   echo -2 "Where"
   echo -2
   echo -2 "   operation   = INSTALL, CHECK or BUILD"
   echo -2 "   packageList = file containing a list of a packages"
   echo -2 "                 to be built, one per line"
   echo -2
   echo -2 "MakeAll will run an INSTALL, CHECK or BUILD against each of  the"
   echo -2 "packages listed in the packageList file.  Each package name"
   echo -2 "should be just the simple name, not decorated by any version"
   echo -2 "number or .tar.gz suffix.  If there are multiple versions of"
   echo -2 "a package, MakeAll will build the latest."
   echo -2
   echo -2 "If MakeAll is interrupted, then restarted with the same operation"
   echo -2 "on the same list of files, it will skip any that have already been"
   echo -2 "run.  (It decides if a file has been run by fgrep'ing for the name"
   echo -2 "in the <operation>.Succeeded and <operation>.Failed lists.)"
   echo -2
   echo -2 "Output:"
   echo -2
   echo -2 "   <operation>.log         Log file"
   echo -2 "   <operation>.Succeeded   List of packages that succeeded"
   echo -2 "   <operation>.Failed      List of packages that failed"
   exit 1
end

local operation, i, tarfile

@ operation = argv[0]
if ( operation != "INSTALL" && operation != "CHECK" && operation != "BUILD" ) then
   echo -2 Usage:  Operation must be either INSTALL, CHECK or BUILD
   exit 1
end

@ list = argv[1]
if ( ! -e $list ) then
   echo -2 Usage: List does not exist
   exit 1
end

touch $operation.Failed $operation.Succeeded $operation.Log
   
(  echo
   echo
   echo -- =======
   echo -- ======= Beginning run at `dt` =======
   echo -- =======
   echo
   echo

   if ( -e $RHome\library\00LOCK ) then
      echo -2 Removing left-over lock at $RHome\library\00LOCK
      hrm -x $RHome\library\00LOCK
   end

   time foreach i ( `cat $list` )


      if ( ! { fgrep -x $i $operation.Failed $operation.Succeeded > nul } ) then

         echo

         echo
         echo -- ======= $operation $i =======

         echo

         echo


         if ( -e ${i}_*.tar.gz ) then

            set tarfile = ${i}_*.tar.gz
            set tarfile = $tarfile:$

            if ( operation != "BUILD" ) then
               time Spawn r CMD $operation $tarfile
            else
            time ( tar --ungzip -xf $tarfile; Spawn r CMD BUILD -binary --no-vignettes $i )
            end
            @ errno = status
         else
            echo -2 ${i}"_*.tar.gz" does not exist
            @ errno = 2
         end

         echo
         echo
         if (errno != 0) then
            echo -- ======= $operation FAILED FOR $i =======
            echo $i >> $operation.Failed
         else
            echo -- ======= $operation SUCCEEDED FOR $i =======
            echo $i >> $operation.Succeeded
         end
         if ( -e $RHome\library\00LOCK ) then
            echo -2 Removing left-over lock at $RHome\library\00LOCK
            hrm -x $RHome\library\00LOCK
         end
         KillOrphans
         echo
         echo

      else

         echo -2 $operation $i has already been run

      end

   end


   echo
   echo
   echo -- =======
   echo -- ======= Ending run at `dt` =======
   echo -- =======
   echo
   echo
   KillOrphans

 ) |& $shell:h\tr -n | tee -a $operation.Log
