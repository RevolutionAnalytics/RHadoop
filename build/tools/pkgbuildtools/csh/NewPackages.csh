if ( $#argv < 2 ) then
   echo -2 "Usage:  NewPackages <oldDirectory> <newDirectory>"
   echo -2
   echo -2 "NewPackages will generate a list of packages in the newDirectory"
   echo -2 "that are different or have been added."
   exit 1
end

local old, new, i

set old = $0
set new = $1

if ( ! -D $old ) then
   echo -2- "$old isn't a directory"
   exit 1
end

if ( ! -D $new ) then
   echo -2- "$new isn't a directory"
   exit 1
end

# Load the sizeof procedure
sizeof nul > nul

(cdd $new
set newfiles = *.tar.gz)

(cdd $old
set oldfiles = *.tar.gz)

foreach i ($newfiles)
   if ( ! -e $old\$i || sizeof($old\$i) != sizeof($new\$i) ) then
      echo -- "$i"
   end
end | sed "s/_.*\.tar\.gz//" | uniq

