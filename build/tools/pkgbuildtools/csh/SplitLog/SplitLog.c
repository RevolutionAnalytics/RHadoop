#include <stdio.h>
#include <windows.h>
#include <stdarg.h>

#define	UsageError		0xff01

HANDLE		Stdout,
			Stderr,
			Logfile,
			PackageLog;

BOOL		EndOfFile = FALSE;

char		*ExeName,
			*LogfileName = NULL,
			FileBuffer[ 64*1024 ],
			*NextChar = NULL,
			*PastLast = NULL,
			LineBuffer[	8*1024 ],
			*LineTextEnd,
			Operation[ 32 ],
			Package[ 128 ],
			ConstructedName[ 512 ];

size_t	LineLength;

#define	EndOfLineBuffer			( LineBuffer + sizeof( LineBuffer ) - 1 )
#define	EndOfLine					( LineBuffer + LineLength - 1 )
#define	EndOfOperation				( Operation + sizeof( Operation ) - 1 )
#define	EndOfPackage				( Package + sizeof( Package ) - 1 )


void Write( HANDLE f, char *text, DWORD length )
	{
	DWORD bytes;
	if ( length == 0 )
		length = ( int )strlen( text );
	WriteFile( f, text, length, &bytes, NULL );
	}


void Message( char *message, ... )
	{
	char *p, c;
	va_list varargs;
	va_start( varargs, message );
	for ( p = message;  c = *p;  message = p )
		{
		for ( ;  ( c = *p ) && c != '%';  p++ )
			;
		Write( Stdout, message, ( DWORD )( p - message ) );
		if ( c == '%' )
			{
			Write( Stdout, va_arg( varargs, char * ), 0 );
			p++;
			}
		}
	
	Write( Stdout, "\r\n", 2 );
	}


void Error( DWORD exitCode, char *message, ... )
	{
	char *p, c;
	va_list varargs;
	va_start( varargs, message );
	Write( Stderr, ExeName, 0 );
	Write( Stderr, ":  ", 3 );
	for ( p = message;  c = *p;  message = p )
		{
		for ( ;  ( c = *p ) && c != '%';  p++ )
			;
		Write( Stderr, message, ( DWORD )( p - message ) );
		if ( c == '%' )
			{
			Write( Stderr, va_arg( varargs, char * ), 0 );
			p++;
			}
		}
	
	Write( Stderr, "\r\n", 2 );
	ExitProcess( exitCode );
	}


#define	OpenKeyboard( )	CreateFileA( "CONIN$",						\
										GENERIC_READ | GENERIC_WRITE,			\
										FILE_SHARE_READ | FILE_SHARE_WRITE,	\
										NULL, OPEN_EXISTING, 0, NULL )

void Pause( )
	{
	HANDLE Keyboard, Stdout;
	char pressEnter[] = "Press Enter to continue ...",
		buffer;
	DWORD bytes;
	Keyboard = OpenKeyboard( );
	Stdout = GetStdHandle( STD_OUTPUT_HANDLE );
	if ( Stdout == INVALID_HANDLE_VALUE )
		ExitProcess( ERROR_INVALID_HANDLE );
	WriteFile( Stdout, pressEnter, sizeof( pressEnter ) - 1, &bytes, NULL );
	ReadFile( Keyboard, ( void * )&buffer, sizeof( buffer ), &bytes, NULL );
	CloseHandle( Keyboard );
	CloseHandle( Stdout );
	}


BOOL FillBuffer( )
	{
	DWORD bytes;
	BOOL readResult;

	if ( NextChar >= PastLast )
		{
		if ( EndOfFile )
			return FALSE;

		readResult = ReadFile( Logfile, FileBuffer, sizeof( FileBuffer ),
				&bytes, NULL );

		if ( !readResult )
			{
			DWORD rc = GetLastError( );
			if ( rc != ERROR_HANDLE_EOF )
				Error( rc, "Read failed on %.", LogfileName );
			}

		PastLast = ( NextChar = FileBuffer ) + bytes;
		if ( bytes == 0 )
			{
			FileBuffer[ 0 ] = 0;
			EndOfFile = TRUE;
			return FALSE;
			}
		}

	return TRUE;
	}


BOOL GetNextChar( char *to )
	{
	if ( FillBuffer( ) )
		{
		char c = *to = *NextChar++;
		return c != '\r' && c != '\n';
		}
	else
		return FALSE;
	}



BOOL GetLine( )
	{
	char *to = LineBuffer;
	if ( FillBuffer( ) )
		{
		while ( to < EndOfLineBuffer && GetNextChar( to++ ) )
			;
		LineTextEnd = to - 1;
		if ( to < EndOfLineBuffer && !EndOfFile )
			{
			if ( FillBuffer( ) )
				{
				// Normalize the line-ends as \r\n.
				if ( to[ -1 ] == '\r' )
					{
					if ( *NextChar == '\n' )
						NextChar++;
					}
				else
					{
					// Must be a newline.
					to[ -1 ] = '\r';
					if ( *NextChar == '\r' )
						// Usually, it's just a \n by itself, UNIX-style,
						// but it could be a reversed \n\r.
						NextChar++;
					}
				*to++ = '\n';
				}
			}
		}

	*to = 0;
	LineLength = to - LineBuffer;
	return !EndOfFile;
	}


BOOL Marker( )
	{
	return strncmp( "=======", LineBuffer, 7 ) == 0 &&
			LineTextEnd - 8 >= LineBuffer &&
			strncmp( " =======", LineTextEnd - 8, 8 ) == 0;
	}


BOOL BeginOperation( )
	{
	if ( Marker( ) )
		{
		char c, *to, *from;
		BOOL skipping = FALSE;

		for ( to = Operation, from = LineBuffer + 8;
				from < EndOfLine && ( c = *from ) && c != ' ' &&
					to < EndOfOperation;  *to++, *from++ )
			*to = c;
		*to = 0;

		if ( strcmp( Operation, "INSTALL" ) == 0 ||
				strcmp( Operation, "CHECK" ) == 0 ||
				strcmp( Operation, "BUILD" ) == 0 )
			{
			for ( to = Package, from++;
					from < EndOfLine && ( c = *from++ ) && c != ' ' &&
						to < EndOfPackage;  )
				switch ( c )
					{
					case '\\':
					case '/':
					case ':':
						// Restart
						to = Package;
						skipping = FALSE;
						break;
					case '-':
					case '_':
						skipping = TRUE;
						break;
					default:
						if ( !skipping )
							*to++ = c;
				}

			*to = 0;
			return TRUE;
			}
		}
	return FALSE;
	}


void main( int argc, char *argv[] )
	{
	int i;

	Stderr = GetStdHandle( STD_ERROR_HANDLE );
	Stdout = GetStdHandle( STD_OUTPUT_HANDLE );
	if ( Stderr == INVALID_HANDLE_VALUE ||
			Stdout == INVALID_HANDLE_VALUE )
		ExitProcess( ERROR_INVALID_HANDLE );
	ExeName = *argv;

	// Skip over any options.  The only options this very dumb cp
	// takes are "--" to mean end-of-options convention and "-@"
	// to mean pause for keystroke before continuing (so as to
	// allow a debugger to be attached.)

	for ( i = 1;  i < argc; i++ )
		{
		char *p = argv[ i ];
		if ( *p++ == '-' )
			{
			if ( *p == '@' && *++p == 0 )
				Pause( );
			else
				if ( *p == '-' && *++p == 0 )
					break;
			}
		else
			break;
		}

	// Foreach Logfile specified in argv.
	for ( ;  i < argc;  i++ )
		{
		FILETIME timestamp;
		SYSTEMTIME date;
		int len;
		DWORD bytes;

		LogfileName = argv[ i ];
		EndOfFile = FALSE;
		NextChar = PastLast = FileBuffer;

		// Open the input log file for reading.
		Message( LogfileName );

		Logfile = CreateFile( LogfileName, GENERIC_READ,
				FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
				OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL );
		if ( Logfile == INVALID_HANDLE_VALUE )
			Error( GetLastError(), "Couldn't open % for reading.", LogfileName );

		// Get the timestamp of the file and transform to a timestamp string.
		if ( !GetFileTime( Logfile, NULL, NULL, &timestamp ) )
			Error( GetLastError(), "Couldn't read the timestamp from %.",
				LogfileName );
			
		if ( !FileTimeToSystemTime( &timestamp, &date ) )
			Error( GetLastError(), "Couldn't transform the timestamp for %.",
				LogfileName );
		
		// While not end-of-file
		while ( !EndOfFile )
			{
			// Scan ahead to the first "======= " + INSTALL/CHECK/BUILD
			// and extract the operation and name of the package.
			while ( GetLine( ) && !BeginOperation( ) )
				;
			if ( !EndOfFile )
				{
				// Open a file of the name <package>.<operation>.<datetime>.log
				len = sprintf_s( ConstructedName, sizeof( ConstructedName ),
					"%s.%s.%04d-%02d-%02d.%02d.%02d.%02d.%03d",
					Package, Operation, date.wYear, date.wMonth, date.wDay,
					date.wHour, date.wMinute, date.wSecond, date.wMilliseconds );

				if ( len < 0 )
					Error( UsageError, "Could not construct output name for %.",
						Package );

				// Message( "> %", ConstructedName );
				PackageLog = CreateFile( ConstructedName, GENERIC_WRITE,
					FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
					CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL );
				if ( PackageLog == INVALID_HANDLE_VALUE )
					Error( GetLastError(), "Couldn't open % for writing.",
						ConstructedName );

				if ( LineLength &&
						!WriteFile( PackageLog, LineBuffer, ( DWORD )LineLength,
							&bytes, NULL ) )
					Error( GetLastError(), "Could write to %.", ConstructedName );

				// While input != "=======" + INSTALL/CHECK/BUILD FAILED/SUCCEEDED
				while ( GetLine( ) && !Marker( ) )
					{
					// Write to the output file
					if ( LineLength &&
							!WriteFile( PackageLog, LineBuffer, ( DWORD )LineLength,
								&bytes, NULL ) )
						Error( GetLastError(), "Could write to %.", ConstructedName );
					}

				// Write that last line to the output file
				if ( LineLength &&
						!WriteFile( PackageLog, LineBuffer, ( DWORD )LineLength,
							&bytes, NULL ) )
					Error( GetLastError(), "Could write to %.", ConstructedName );

				// Set the timestamp on it.
				SetFileTime( PackageLog, NULL, NULL, &timestamp );

				// Close the output file
				CloseHandle( PackageLog );
				}
			}
			
		// Close the input file
		CloseHandle( Logfile );
		}
	}
