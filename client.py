import socket, io, struct, time

def normal():
    HOST, IP = '127.0.0.1', 9049
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, IP))
    AGG_FINISH = b'\x00\x00'
    BUFFER_SIZE = 104857600 # 100MB
    fs = io.open('client_1-latest.pth', 'ab+')
    fsize = fs.tell()
    print("file size is {}".format(fsize))
    for epoch in range(100):
        # 设置头
        # ssize: 8, client_id: 8, epoch: 4, precision: 4
        header = struct.pack("<Q8sIf", fsize, b'client1', epoch, 89.64)
        header += b'\x00' * 8    # reserved: 8
        print("header is {}".format(header))
        s.sendall(header)
        fs.seek(0)
        time.sleep(0.5)
        print("start upload...")
        while True:
            bytes_read = fs.read(BUFFER_SIZE)
            if not bytes_read:
                # EOF
                break

            # we use sendall to assure transimission in
            # busy networks
            s.sendall(bytes_read)
        print("upload finish...")

        t1 = time.time()
        is_agg_finish = s.recv(2)      # 应该阻塞在这里了，直到 server 聚合完成
        t2 = time.time()
        print("wait {} seconds to agg_finish, recv:  {}".format(t2 - t1, is_agg_finish))
        if is_agg_finish != AGG_FINISH:
            print("Failed to upload!")
            exit(-1)
        ssize = s.recv(8)
        obj_len = struct.unpack("<Q", ssize)[0]

        print("Start recv, should recv {} bytes".format(obj_len))
        BUFFER_SIZE = 1 << 22
        buf = io.BytesIO()
        while True:
            try:
                while buf.tell() < obj_len:
                    resume = obj_len - buf.tell()
                    if resume < BUFFER_SIZE:
                        bytes_read = s.recv(resume)
                    else:
                        bytes_read = s.recv(BUFFER_SIZE)
                    if not bytes_read:
                        print("Break")
                        break
                    buf.write(bytes_read)
                    # print("Recv {}/{} bytes".format(buf.tell(), obj_len))
            except Exception as e:
                print('except: {}'.format(e))
            if buf.tell() == obj_len:
                print("Recv {} bytes finally".format(buf.tell()))
                break
        ss = input(">>")
        if ss == 'q':
            break
        # extra_data = s.recv(1024)
        # if extra_data:
        #     print("Extra data recvd: {}".format(extra_data))


if __name__ == '__main__':
    normal()
