if ( $#argv == 0 ) then
   echo -2 "Usage:  MakePatched <patchfiles>"
   echo -2
   echo -2 "For each of the specified patch files, apply the patch to"
   echo -2 "the corresponding package in the current directory, then"
   echo -2 "run INSTALL, CHECK and BUILD against it.  Each patch file"
   echo -2 "name should be a complete (albeit perhaps relative) path"
   echo -2 "to the file, not simply a package name."
   exit 1
end

local i, package, tarfile

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

   time foreach i ( $argv:gf )

      if ( ! -e $i ) then
         echo -2 $i patch file does not exist
         exit 1
      end

      @ package = `echo $i:t | sed 's/_.*$//'`
      @ tarfile = $i:t:s/patch/tar.gz/

      echo
      echo
      echo -- ======= Patching $package =======
      echo
      echo

      if ( -e $tarfile ) then

         tar --ungzip -xf $tarfile
         (cd $package; patch -p1 --binary < $i)

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
               echo -- ======= $operation FAILED FOR $i =======
               echo $package >> Patch.$operation.Failed
               # Once we get a failure, we stop building this package
               break
            else
               echo -- ======= $operation SUCCEEDED FOR $i =======
               echo $package >> Patch.$operation.succeeded
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
         echo -2 $tarfile does not exist
         @ errno = 2
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

 ) |& $shell:h\tr -n | tee -a Patch.Log
