if ( $#argv < 2 ) then
   echo -2 "Usage:  SummarizePackages <oldDirectory> <newDirectory>"
   echo -2
   echo -2 "SummarizePackages will summarize the differences between the sets of"
   echo -2 "packages in an old and a new directory, listing those which are the"
   echo -2 "same, those that have the same version number but a different size,"
   echo -2 "those that have a different version number, and those which have been"
   echo -2 "added or deleted."
   echo -2
   echo -2 "If there are multiple versions of a given package in a directory,"
   echo -2 "the comparison will use the latest (by version number.)"
   exit 1
end

local Old, OldPackages, OldFiles, New, NewPackages, NewFiles, i, j, f, list, AllNewFiles, AllOldFiles

set Old = $0
set New = $1

if ( ! -D $Old ) then
   echo -2- "$Old isn't a directory"
   exit 1
end

if ( ! -D $New ) then
   echo -2- "$New isn't a directory"
   exit 1
end

# Load the sizeof and member procedures
sizeof nul > nul
member > nul

(cdd $New
set AllNewFiles = *.tar.gz
set NewPackages = `foreach i ( *.tar.gz ) echo $i; end | sed "s/_.*\.tar\.gz//" | sort -f | uniq`
foreach i ( $NewPackages )
	set f = ${i}*.tar.gz
	set NewFiles = $NewFiles $f:$
end)

(cdd $Old
set AllOldFiles = *.tar.gz
set OldPackages = `foreach i ( *.tar.gz ) echo $i; end | sed "s/_.*\.tar\.gz//" | sort -f | uniq`
foreach i ( $OldPackages )
	set f = ${i}*.tar.gz
	set OldFiles = $OldFiles $f:$
end)

echo =====
echo ===== Same Latest Version ======
echo =====
echo

set list = `foreach i ( $NewFiles ) ^
   if ( -e $Old\$i && sizeof( $Old\$i ) == sizeof( $New\$i ) ) then ^
      calc i; ^
   end; ^
end | sed "s/_.*\.tar\.gz//" | uniq`

@ j = 1
foreach i ( $list )
   echo -- "$j^t$i"
   @ j++
end

echo
echo =====
echo ===== Same Latest Version, Different Size ======
echo =====
echo

@ j = 1
set list = `foreach i ( $NewFiles ) ^
   if ( -e $Old\$i && sizeof( $Old\$i ) != sizeof( $New\$i ) ) then ^
      calc i; ^
   end; ^
end | sed "s/_.*\.tar\.gz//" | uniq`

@ j = 1
foreach i ( $list )
   echo -- "$j^t$i"
   @ j++
end

echo
echo =====
echo ===== Same Version (but not the latest), Different Size ======
echo =====
echo

@ j = 1
foreach i ( $AllNewFiles )
   if ( -e $Old\$i && ! member( i, NewFiles ) && sizeof( $Old\$i ) != sizeof( $New\$i ) ) then
      echo -- "$j^t$i"
      @ j++
   end
end

echo
echo =====
echo ===== Different Version ======
echo =====
echo

@ j = 1
foreach i ( $NewFiles )
   if ( ! -e $Old\$i ) then
      set f = `echo $i | sed "s/_.*\.tar\.gz//"`
      if ( member( f, OldPackages ) ) then
         echo -- "$j^t$f"
         @ j++
      end
   end
end

echo
echo =====
echo ===== Added Packages ======
echo =====
echo

@ j = 1
foreach i ( $NewPackages )
	if ( ! member( i, OldPackages ) ) then
      echo -- "$j^t$i"
      @ j++
   end
end

echo
echo =====
echo ===== Deleted Packages ======
echo =====
echo

@ j = 1
foreach i ( $OldPackages )
   if ( ! member( i, NewPackages ) ) then
      echo -- "$j^t$i"
      @ j++
   end
end
