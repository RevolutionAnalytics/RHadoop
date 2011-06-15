if ( $#argv == 0 ) then
   echo -2 "Usage:  MakeFromNewTar <targzfiles>"
   echo -2
   echo -2 "For each of the specified tar.gz files, ungzip, use the Hamilton"
   echo -2 "tar to untar, then run INSTALL, CHECK and BUILD.  Each tar.gz file"
   echo -2 "name should be a complete (albeit perhaps relative) path"
   echo -2 "to the file, not simply a package name."
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

   time foreach i ( $argv )

      if ( ! -e $i ) then
         echo -2 $i tar.gz file does not exist
         exit 1
      end

      @ package = `echo $i:t | sed 's/_.*$//'`

      echo
      echo
      echo -- ======= Untarring $package =======
      echo
      echo

      set tarfile = $i:s/.gz//
      gzip -d - < $i > $tarfile
      $shell:h\tar -qxr $tarfile

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
            echo $package >> $operation.Failed
            # Once we get a failure, we stop building this package
            break
         else
            echo -- ======= $operation SUCCEEDED FOR $i =======
            echo $package >> $operation.succeeded
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

 ) |& $shell:h\tr -n | tee -a MakeFromNewTar.Log
