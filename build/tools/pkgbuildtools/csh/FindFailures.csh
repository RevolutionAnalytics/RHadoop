if ( $#argv == 0 ) then
   echo -2 "Usage:  FindFailures <directory>"
   echo -2
   echo -2 "FindFailures will scan the specified directory, looking for split"
   echo -2 "log files having names of the form <package>.<operation>.<timestamp>."
   echo -2 "If there's an INSTALL but no CHECK log file, FindFailures infers"
   echo -2 "that the INSTALL must have failed.  If there's a CHECK but no BUILD"
   echo -2 "log file, it infers that the CHECK must have failed."
   echo -2
   echo -2 "Output:"
   echo -2
   echo -2 "   INSTALL.Failed        List of modules that failed in INSTALL"
   echo -2 "   CHECK.Failed          List of modules that failed in CHECK"
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

proc AnotB( A, B )
   return `( foreach i ( $A ) echo $i; end; foreach i ( $B ) echo $i; end ) | sort -f  | uniq -u`
end

local InstallsRun, ChecksRun, BuildsRun

(cd $argv[0]

@ InstallsRun = ModulesRun( "INSTALL" )
@ ChecksRun = ModulesRun( "CHECK" )
@ BuildsRun = ModulesRun( "BUILD" )
)

@ FailedInInstall = AnotB( InstallsRun, ChecksRun )
@ FailedInCheck = AnotB( ChecksRun, BuildsRun )

foreach i ( $FailedInInstall )
   echo $i
end > INSTALL.Fail

foreach i ($FailedInCheck )
   echo $i
end > CHECK.Fail

