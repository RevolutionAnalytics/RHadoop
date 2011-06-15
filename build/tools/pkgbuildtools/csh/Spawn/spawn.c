#include <windows.h>
#include <tlhelp32.h>
#include <shlwapi.h>
#include <stdio.h>


typedef struct Process_str
	{
	DWORD ProcessID;
	struct Process_str *Next, *Sibling, *Child;
	}	Process;

#define	HashSize		1024
Process *ProcessHash[HashSize];

CRITICAL_SECTION exitOwner;


PROCESS_INFORMATION ChildTree;


void ZeroHash( )
	{
	int i;
	for ( i = 0;  i < HashSize;  i++ )
		ProcessHash[ i ] = NULL;
	}


Process *AllocateProcess( )
	{
	return calloc( sizeof( Process ), 1 );
	}


void FreeProcess( Process *p )
	{
	free( p );
	}


void EmptyHash( )
	{
	int i;
	for ( i = 0;  i < HashSize;  i++ )
		{
		Process *p, *n;
		for ( p = ProcessHash[ i ];  p;  p = n )
			{
			n = p->Next;
			FreeProcess( p );
			}
		ProcessHash[ i ] = NULL;
		}
	}


void InsertProcess( Process *p )
	{
	p->Next = ProcessHash[ p->ProcessID % HashSize ];
	ProcessHash[ p->ProcessID % HashSize ] = p;
	}


Process *FindProcess( DWORD processID )
	{
	Process *p = ProcessHash[ processID % HashSize ];
	while ( p && p->ProcessID != processID )
		p = p->Next;
	if ( p == NULL )
		{
		p = AllocateProcess( );
		p->ProcessID = processID;
		InsertProcess( p );
		}
	return p;
	}


void AddChild( Process *parent, Process *child )
	{
	child->Sibling = parent->Child;
	parent->Child = child;
	}


void KillProcess( Process *p )
	{
	HANDLE *h = OpenProcess( PROCESS_TERMINATE, FALSE, p->ProcessID );
	if ( h )
		TerminateProcess( h, 1 );
	}
		

void KillTree( Process *p )
	{
	Process *child;
	KillProcess( p );
	for ( child = p->Child;  p;  p = p->Sibling )
		KillTree( child );
	}


Process *GetProcessList( )
	{
	// Take a snapshot of all processes in the system and return a pointer
	// to the WerFault process if there is one.
	HANDLE snapshot;
	PROCESSENTRY32 processEntry;
	Process *werFault = NULL;

	snapshot = CreateToolhelp32Snapshot( TH32CS_SNAPPROCESS, 0 );
	if( snapshot == INVALID_HANDLE_VALUE )
		return NULL;

	// Set the size of the structure before using it.
	processEntry.dwSize = sizeof( PROCESSENTRY32 );

	// Retrieve information about the first process,
	// and exit if unsuccessful
	if( !Process32First( snapshot, &processEntry ) )
		{
		CloseHandle( snapshot );          // clean the snapshot object
		return NULL;
		}

	// Now walk the snapshot of processes, and
	// display information about each process in turn
	do
		{
		Process *p, *parent;
		DWORD processID = processEntry.th32ProcessID,
			parentProcessID = processEntry.th32ParentProcessID;
		char *name = processEntry.szExeFile;
		p = FindProcess( processID );
		parent = FindProcess( parentProcessID );
		AddChild( parent, p );
		if ( StrCmpNI( name, "WerFault", 8 ) == 0 &&
				( name[ 8 ] == 0 || name[ 8 ] == '.' ) )
			werFault = p;
		}
	while( Process32Next( snapshot, &processEntry ) );

	CloseHandle( snapshot );
	return werFault;
	}


void watcher( HANDLE *child )
	{
	// Every 15 seconds, look to see if there's a WerFault process.  If one is found,
	// terminate it, the child process and any children of the child.

	Process *werFault, *childProcess;
	ZeroHash( );

	while ( TRUE )
		{
		EmptyHash( );
		werFault = GetProcessList( );
		if ( werFault )
			{
			// Our child has crashed.  Kill both the WerFault and child trees, then
			// exit.
			EnterCriticalSection( &exitOwner );
#			ifdef	Debugging
				printf( "killing WerFault\n" );
				_flushall();
#			endif
			KillProcess( werFault );
			childProcess = FindProcess( ChildTree.dwProcessId );
			if ( childProcess )
				{
#				ifdef	Debugging
					printf( "killing child\n" );
					_flushall();
#				endif
				KillTree( childProcess );
				}
			ExitProcess( 1 );
			}
		Sleep( 15000 );
		}
	}


void cdecl main( void )
   {
	DWORD exitCode = 0;
   char *c = GetCommandLine( );

   while ( *c && *c != ' ' )
      c++;
   while ( *c == ' ' )
      c++;

   if ( *c )
      {
      static STARTUPINFO startup;
      startup.cb = sizeof( startup );

		SetErrorMode( SEM_FAILCRITICALERRORS |
				SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX );

      if ( CreateProcessA( NULL, c, NULL, NULL, TRUE /* Inherit handles */,
            0, NULL, NULL, &startup, &ChildTree ) )
			{
			DWORD childid;
			HANDLE child;
			CloseHandle( ChildTree.hThread );
			InitializeCriticalSection( &exitOwner );
			child = CreateThread( NULL, 4096, ( LPTHREAD_START_ROUTINE )watcher,
				NULL, 0, &childid );
			CloseHandle( child );
	      WaitForSingleObject( ChildTree.hProcess, INFINITE );
			GetExitCodeProcess( ChildTree.hProcess, &exitCode );
			}
		else
			{
			exitCode = GetLastError( );
			printf( "Could not spawn '%s'\n", c );
			}
      }
   else
      printf( "usage: spawn command\n" );

#	ifdef Debugging
		printf( "About to exit.\n" );
		_flushall();
#	endif

	EnterCriticalSection( &exitOwner );

#	ifdef Debugging
	printf( "Exiting now.\n" );
	_flushall();
#	endif

   ExitProcess( exitCode );
   }
