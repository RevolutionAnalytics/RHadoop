if ( $#argv == 0 ) then
   echo -2 "Usage:  MakeOne <packages>"
   echo -2
   echo -2 "Run INSTALL, CHECK and BUILD against each of the specified"
   echo -2 "packages.  Each package name should be just the simple name,"
   echo -2 "not decorated by version number or any .tar.gz suffix.  If"
   echo -2 "there are multipler versions of a package, MakeOne will build"
   echo -2 "the latest."
   exit 1
end

local operation, i, tarfile

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

   time foreach i ( $argv )

      if ( -e ${i}_*.tar.gz ) then

         set tarfile = ${i}_*.tar.gz
         set tarfile = $tarfile:$

         foreach operation ( INSTALL CHECK BUILD )

            echo
            echo
            echo -- ======= $operation $i =======
            echo
            echo

            if ( operation != "BUILD" ) then
               time Spawn r CMD $operation $tarfile
            else
                time ( tar --ungzip -xf $tarfile; Spawn r CMD BUILD -binary --no-vignettes $i )
            end
            @ errno = status

            echo
            echo
            if (errno != 0) then
               echo -- ======= $operation FAILED FOR $i =======
               echo $i >> MakeOne.$operation.Failed
               # Once we get a failure, we stop building this package
               break
            else
               echo -- ======= $operation SUCCEEDED FOR $i =======
               echo $i >> MakeOne.$operation.succeeded
            end
            if ( -e $RHome\library\00LOCK ) then
               echo -2 Removing left-over lock at $RHome\library\00LOCK
               hrm -x $RHome\library\00LOCK
            end
            KillOrphans
            echo
            echo
         end

      else
         echo -2 ${i}"_*.tar.gz" does not exist
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

 ) |& $shell:h\tr -n | tee -a MakeOne.Log
