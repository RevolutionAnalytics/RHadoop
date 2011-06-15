if ( $#argv == 0 ) then
   echo -2 "Usage:  MakePkgDir <PkgDirectory>"
   echo -2
   echo -2 "For each of the specified package directories, run INSTALL,"
   echo -2 "CHECK and BUILD against it."
   exit 1
end

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

   time foreach package ( $argv )

      if ( ! -e $package ) then
         echo -2 $package does not exist
         exit 1
      end

      foreach operation ( INSTALL CHECK BUILD )

         echo
         echo
         echo -- ======= $operation $package =======
         echo
         echo

         if ( operation != "BUILD" ) then
            time Spawn r CMD $operation $package
         else
             time Spawn r CMD BUILD -binary --no-vignettes $package
         end
         @ errno = status

         echo
         echo
         if (errno != 0) then
            echo -- ======= $operation FAILED FOR $package =======
            echo $package >> PkgDir.$operation.Failed
            # Once we get a failure, we stop building this package
            break
         else
            echo -- ======= $operation SUCCEEDED FOR $package =======
            echo $package >> PkgDir.$operation.succeeded
         end

         if ( -e $RHome\library\00LOCK ) then
            echo -2 Removing left-over lock at $RHome\library\00LOCK
            hrm -x $RHome\library\00LOCK
         end
         KillOrphans

         echo
         echo
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

 ) |& $shell:h\tr -n | tee -a MakePkgDir.Log
