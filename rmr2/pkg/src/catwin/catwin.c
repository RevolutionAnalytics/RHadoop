#include <stdio.h>
#include <stdlib.h>
#include <io.h>
#include <sys/stat.h>
#include <fcntl.h>

#define CAT_BUFSIZ 4096

int main(int argc, char* argv[])
{
	int wfd;
	int rfd;
	ssize_t nr, nw, off;
	static char *buf = NULL;
	static char fb_buf[CAT_BUFSIZ];
	static size_t bsize;

	rfd = fileno(stdin);
	wfd = fileno(stdout);
	
	setmode(rfd, O_BINARY);
	setmode(wfd, O_BINARY);
	
	buf = fb_buf;
	bsize = CAT_BUFSIZ;

	while ((nr = read(rfd, buf, bsize)) > 0)
		for (off = 0; nr; nr -= nw, off += nw)
			nw = write(wfd, buf + off, (size_t)nr);

	fclose(stdout);
	return 0;
}
