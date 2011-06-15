if ( $#argv == 0 ) then
   echo -2 "Usage:  FindSuccesses <directory>"
   echo -2
   echo -2 "FindSuccesses will scan the specified directory, looking for split"
   echo -2 "log files having names of the form <package>.<operation>.<timestamp>."
   echo -2 "If both an INSTALL and a CHECK log file exists, FindSuccesses infers"
   echo -2 "that the INSTALL must have succeeded.  If both a CHECK and a BUILD"
   echo -2 "log file exists, it infers that the CHECK must have succeeded."
   echo -2
   echo -2 "Output:"
   echo -2
   echo -2 "   INSTALL.Succeeded     List of modules that succeeded in INSTALL"
   echo -2 "   CHECK.Succeeded       List of modules that succeeded in CHECK"
   exit 1
end

proc UniqOperation( operation )
   foreach i (*.${operation}.* )
      echo $i
   end | sed "s/\.${operation}\..*//" | uniq
end

proc ModulesRun( operation )
   return `UniqOperation $operation`
end

proc AandB( A, B )
   return `( foreach i ( $A ) echo $i; end; foreach i ( $B ) echo $i; end ) | sort -f  | uniq -d`
end

local InstallsRun, ChecksRun, BuildsRun

(cd $argv[0]

@ InstallsRun = ModulesRun( "INSTALL" )
@ ChecksRun = ModulesRun( "CHECK" )
@ BuildsRun = ModulesRun( "BUILD" )
)

@ SucceededInInstall = AandB( InstallsRun, ChecksRun )
@ SucceededInCheck = AandB( ChecksRun, BuildsRun )

foreach i ( $SucceededInInstall )
   echo $i
end > INSTALL.Succeeded

foreach i ($SucceededInCheck )
   echo $i
end > CHECK.Succeeded

