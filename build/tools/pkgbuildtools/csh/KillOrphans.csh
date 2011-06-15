proc FindOrphans( )
   ps -s | sed -n 's/^^\(p[0-9][0-9]*\)  *make .*$/\1/p; ^
                     s/^^\(p[0-9][0-9]*\)  *sh .*$/\1/p; ^
                     s/^^\(p[0-9][0-9]*\)  *Rterm .*$/\1/p'
end

proc KillOrphans( )
   local orphans
   set orphans = `FindOrphans`
   if ( $#orphans ) then
      echo -2 Killing $#orphans orphan processes.
      kill -x! $orphans
   end
end
   
KillOrphans
