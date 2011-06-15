(  echo
   echo
   echo -- =======
   echo -- ======= Beginning run at `dt` =======
   echo -- =======
   echo
   echo

   local operation, tarfile

   if ( -e $RHome\library\00LOCK ) then
      echo -2 Removing left-over lock at $RHome\library\00LOCK
      hrm -x $RHome\library\00LOCK
   end

   time (

      if ( -e RWinEdt_*.tar.gz ) then

         set tarfile = RWinEdt_*.tar.gz
         set tarfile = $tarfile:$

         foreach operation ( INSTALL CHECK BUILD )

            echo
            echo
            echo -- ======= $operation RWinEdt =======
            echo
            echo

            switch ( operation )
               case "INSTALL":
                  time Spawn r CMD INSTALL $tarfile
                  break
               case "CHECK":
                  time Spawn r CMD CHECK --install=fake $tarfile
                  break
               default:      # BUILD
                  time ( tar --ungzip -xf $tarfile; Spawn r CMD BUILD -binary --no-vignettes RWinEdt )
            end

            @ errno = status

            echo
            echo
            if (errno != 0) then
               echo -- ======= $operation FAILED FOR RWinEdt =======
               echo RWinEdt >> MakeOne.$operation.Failed
               # Once we get a failure, we stop building this package
               break
            else
               echo -- ======= $operation SUCCEEDED FOR RWinEdt =======
               echo RWinEdt >> MakeOne.$operation.succeeded
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
         echo -2 "RWinEdt_*.tar.gz" does not exist
      end

   )

   echo
   echo
   echo -- =======
   echo -- ======= Ending run at `dt` =======
   echo -- =======
   echo
   echo
   KillOrphans

 ) |& Htr -n | tee -a MakeOne.Log
