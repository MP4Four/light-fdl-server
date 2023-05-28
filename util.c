#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

/**
 * This program is a server that only `listen' but not recv
 * Aim to view the behaviour of `send' under different mode
 */

int main(int argc, char* argv[])
{
    // 1.Create a listen socket
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1)
    {
        printf("create listen socket error.");
        return -1;
    }

    // 2.Init server address
    struct sockaddr_in bindaddr;
    bindaddr.sin_family = AF_INET;
    bindaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    bindaddr.sin_port = htons(3000);
    if (bind(listenfd, (struct sockaddr *)&bindaddr, sizeof(bindaddr)) == -1)
    {
        printf("bind listen socket error.");
        close(listenfd);
        return -1;
    }

    // 3.start listen
    if (listen(listenfd, SOMAXCONN) == -1)
    {
        printf("listen error." );
        close(listenfd);
        return -1;
    }

    while (1)
    {
        struct sockaddr_in clientaddr;
        socklen_t clientaddrlen = sizeof(clientaddr);
        // 4. accpet client connection
        int clientfd = accept(listenfd, (struct sockaddr *)&clientaddr, &clientaddrlen);
        if (clientfd != -1)
        {
            // only accept, does not recv any data
            printf("accept a client connection.\n");
        }
    }

    close(listenfd);

    return 0;
}
