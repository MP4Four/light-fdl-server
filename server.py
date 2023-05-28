import selectors, socket, logging, os, time, io, torch, struct

def init_logger():
    # 创建一个 logging 实例，配置好 log 打印的格式，返回 handle
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    # set two handlers
    filehandler = logging.FileHandler(os.path.join(os.path.curdir, 'server.log'), mode='a')
    filehandler.setLevel(logging.DEBUG)
    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.DEBUG)
    # set formatter
    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)-6s - %(filename)-8s : %(lineno)s line - %(message)s")
    streamhandler.setFormatter(formatter)
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    logger.addHandler(streamhandler)

    logger.info("logger init finish")
    return logger

class Client():

    def __init__(self, ssize=0, cid='', epoch=0, prec=0.0):
        # 模型大小    client id    epoch   precision
        self.ssize, self.cid, self.epoch, self.prec = ssize, cid, epoch, prec

        # 设置模型内存缓冲区，来自于 requests
        self.mbuff = io.BytesIO()

        # 记录是否聚合完成, 根据协议，聚合完成后需要向 client 返回`AGG_FINISH: \x00\x00' 字段
        self.is_agg_finish = False

        # 完成一个周期的上传和下发 后 需要重新解析 header
        self.need_parse_header = True

    def recv(self, stream):
        self.mbuff.write(stream)

    # wrap io.BytesIO tell()
    def tell(self):
        return self.mbuff.tell()

    # 接收缓冲区是否为 0
    def is_empty(self):
        return self.mbuff.tell() == 0

    # 判断是否接收完成
    def is_recv_ok(self):
        return self.mbuff.tell() == self.ssize

    # 聚合完成一轮，清空缓冲区
    def flush(self):
        self.mbuff = io.BytesIO()
        self.need_parse_header = True
        self.is_agg_finish = True

class AggServer():

    def __init__(self, host, port):
        # 初始化 listen socket: lsock
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 防止出现 `Address already in use' 错误
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # bind and listen
        self.lsock.bind((host, port))
        self.lsock.listen(100)
        self.lsock.setblocking(0)   # 设置为非阻塞

        # 设置调用 socket.send() 方法时不用 buffer
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # 创建当前平台下的 I/O multiplexing 机制对象
        self.sel = selectors.DefaultSelector()

        # 注册 read 事件
        self.sel.register(self.lsock, selectors.EVENT_READ,
                          data=self.on_accept)

        # 记录peername     请求 client
        self.peername, self.clients  = {}, {}

        # 记录响应数据已发送多少
        self.responses = {}

        # 记录 connection socket 以供 broadcast
        self.connsock = set()

        # 接收缓冲区设为1MB
        self.BUFFER = 1 << 20

        # 获得 logger 对象
        self.logger = init_logger()

        # 聚合时机控制，等待队列   AND   完成队列
        self.finishq, self.waitq = set(), set()

        # 聚合后的全局模型 -- unpickled
        self.AVG_MODEL = io.BytesIO()
        # self.TEST_MODEL = io.open('dt_datasets.zip', 'rb')

    def on_accept(self, sock, mask):
        """
            此函数作用于 lsock
            用来 accept 新的 client 连接
        """
        # 完成 TCP 握手，建立连接
        conn, addr = sock.accept()
        self.logger.info('accepted connection from {0}'.format(addr))

        # 设置为非阻塞
        conn.setblocking(0)

        # 记录当前新建连接信息以供将来查询
        self.peername[conn.fileno()] = conn.getpeername()

        # 初始化此 socket 的接收缓冲区
        self.clients[conn.fileno()] = Client()

        # 记录此 socket 已发送响应的长度
        self.responses[conn.fileno()] = 0

        # 将此连接加入 socket 事件汇总中
        self.connsock.add(conn)

        # 注册 READ 事件 和 回调函数
        self.sel.register(fileobj=conn, events=selectors.EVENT_READ,
                          data=self.on_read)
        # self.sel.register(fileobj=conn, events=selectors.EVENT_WRITE,
        #                   data=self.on_write)

    def on_read(self, conn, mask):

        # 当有新数据时调用此函数
        try:
            while True:
                data = conn.recv(self.BUFFER)
                """
                    如果对端关闭了 socket 连接，并且 server 将读缓冲区内数据读取完成，那么 recv() 函数会立即完成并且返回0字节
                """
                if not data:
                    self.close_connection(conn)
                    return

                '''
                    为新 session 则需要重新 parse head
                    否则说明是由于设备间速率不匹配原因导致进入了socket.error
                '''
                client = self.clients[conn.fileno()]
                if client.need_parse_header:


                    # 解析 client 包头
                    client.ssize, client.cid, client.epoch, client.prec = struct.unpack("<Q8sIf", data[:24])
                    # reserved = data[24: 32]
                    self.logger.info("Start recv {}'s gradient".format(client.cid))

                    # 添加当前 client id 至等待聚合队列中
                    self.waitq.add(client.cid)

                    # 设置字段，告诉下次就不必重新解析 header 了
                    client.need_parse_header = False

                    # 接收 stream 至 Client 的 mbuff
                    client.recv(data[32:])
                elif client.is_recv_ok():

                    # 已经接收完了确还有残留的数据，证明 client 有问题
                    self.logger.critical("Attacked by client: {}".format(
                        client.cid))

                    # 将有问题的 client socket 从内存中抹去
                    self.waitq.remove(client.cid)
                    self.close_connection(conn)

                    # 记得返回，以免走到后面的逻辑去
                    return
                else:

                    # 某个 client 数据量太大，触发了异常，继续接收
                    client.recv(data)

        except ConnectionResetError:
            self.logger.critical("Client shutdown")
            self.close_connection(conn)
            return

        except socket.error as e:
            # self.logger.debug("socket error: {}".format(e))
            if self.clients[conn.fileno()] is not None:
                self.logger.debug("Recv {} bytes from {}".format(
                    self.clients[conn.fileno()].tell(),
                    self.peername[conn.fileno()]))

        # 此时客户端上传梯度完成
        if self.clients[conn.fileno()].is_recv_ok():
            cid = self.clients[conn.fileno()].cid
            self.waitq.remove(cid)
            self.logger.info("Recv {}'s gradient ok".format(cid))
            self.finishq.add(cid)
            self.try_aggeration()

    def on_write(self, conn, mask):

        # 监测到 WRITE 事件，给 client 响应回复
        try:
            if self.clients[conn.fileno()].is_agg_finish:
                conn.send(b'\x00\x00')
                self.clients[conn.fileno()].is_agg_finish = False

            # peername = self.peername[conn.fileno()]

            # 引用全局模型
            data = self.AVG_MODEL

            # 记录已经传输了多少 bytes
            start = self.responses[conn.fileno()]

            # 得到共需要传输的 bytes 数
            obj_len = data.seek(0, 2)

            client = self.clients[conn.fileno()]

            if start == 0:
                # 根据协议，需要先告知对方共需传输的 bytes 数
                ssize = struct.pack("<Q", obj_len)
                ss = conn.send(ssize)
                data.seek(0, 0)
            elif start == obj_len:
                # 下发完毕
                self.logger.info("Send to {} finish!".format(client.cid))

                # 恢复读事件
                self.sel.modify(conn, selectors.EVENT_READ, data=self.on_read)

                # 设置解析头字段
                client.need_parse_header = True

                # 已经传输了多少 bytes 恢复为 0
                self.responses[conn.fileno()] = 0

                return
            else:
                data.seek(start, 0)
            BUFFER = 1 << 20
            self.logger.debug("Start at {}".format(self.responses[conn.fileno()]))
            # Core logic for slide window
            while True:
                resume = obj_len - start
                if resume == 0:
                    sent = 0 # finally
                    self.logger.info("Send to {} finish!".format(client.cid))
                    self.sel.modify(conn, selectors.EVENT_READ, data=self.on_read)
                    client.need_parse_header = True
                    # 已经传输了多少 bytes 恢复为 0
                    self.responses[conn.fileno()] = 0
                    return
                elif resume < BUFFER:
                    bytes_read = data.read(resume)
                else:
                    bytes_read = data.read(BUFFER)
                if not bytes_read:
                    # EOF
                    self.logger.critical("Break")
                    self.sel.modify(conn, selectors.EVENT_READ, data=self.on_read)
                    break
                sent = conn.send(bytes_read)
                self.logger.debug("Sent {} bytes, read {} bytes ".format(sent, data.tell() - start))
                start += sent
                self.responses[conn.fileno()] = start
                data.seek(start)
                if obj_len != start:
                    self.logger.debug("Next around should seek to {}".format(start))
        except socket.error as e:
            self.logger.debug("Exception: {}.Sent {} bytes, read {} bytes ".format(e, sent, data.tell() - start))
            self.sel.modify(conn, selectors.EVENT_WRITE, data=self.on_write)

    def close_connection(self, conn):
        peername = self.peername[conn.fileno()]
        self.logger.info('closing connection: {0}'.format(peername))
        del self.peername[conn.fileno()]
        del self.clients[conn.fileno()]
        del self.responses[conn.fileno()]
        self.connsock.remove(conn)
        self.sel.unregister(conn)
        conn.close()

    def aggregate(self):

        MODEL = None
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        for client in self.clients.values():
            client.mbuff.seek(0)
            cur_model = torch.load(client.mbuff, map_location=device)
            if MODEL is None:
                MODEL = cur_model

                # 清空 client 缓冲区
                client.flush()
                continue
            with torch.no_grad():
                if hasattr(MODEL, 'named_parameters'):
                    for name, param in MODEL.named_parameters():
                        param.add_(cur_model.state_dict()[name])
                elif hasattr(MODEL, 'parameters'):
                    pass
                elif isinstance(MODEL, dict):
                    for name, param in MODEL['state_dict'].items():
                        param.add_(cur_model['state_dict'][name])

                else:
                    self.logger.error("MODEL type is {}".format(type(MODEL)))
            # 清空 client 缓冲区
            client.flush()
        length = len(self.clients)
        with torch.no_grad():
            if hasattr(MODEL, 'named_parameters'):
                for name, param in MODEL.named_parameters():
                    param.div_(torch.tensor(length, dtype=param.dtype), rounding_mode='trunc')
            elif isinstance(MODEL, dict):
                for name, param in MODEL['state_dict'].items():
                    param.div_(torch.tensor(length, dtype=param.dtype), rounding_mode='trunc')
        self.AVG_MODEL = io.BytesIO()
        torch.save(MODEL, self.AVG_MODEL)
        del MODEL
        self.logger.info("Gradient aggregate finish!")

        # 注册写事件
        for conn in self.connsock:

            cid = self.clients[conn.fileno()].cid
            if cid in self.finishq:
                self.logger.info("Start send aggregated gradient to {}".format(cid))
                self.sel.modify(conn, selectors.EVENT_WRITE, data=self.on_write)
                self.finishq.remove(cid)

        # 收尾清空，防止中途新加的 client 捣乱
        self.finishq.clear()
        self.waitq.clear()

    def test_agg(self):
        self.AVG_MODEL = list(self.clients.values())[0].mbuff

        # 注册写事件
        for conn in self.connsock:
            # 设置聚合已经完成标识
            self.clients[conn.fileno()].is_agg_finish = True

            cid = self.clients[conn.fileno()].cid
            if cid in self.finishq:
                self.sel.modify(conn, selectors.EVENT_WRITE, data=self.on_write)
                self.finishq.remove(cid)

        # 收尾清空，防止中途新加的 client 捣乱
        self.finishq.clear()


    def try_aggeration(self):

        if len(self.finishq) == len(self.peername):  # != 1
            self.aggregate()

    def serve_forever(self):
        last_report_time = time.time()
        while True:
            # Wait until some registered socket becomes ready. This will block
            # for 200 ms.
            events = self.sel.select(timeout=0.2)

            # For each new event, dispatch to its handler
            for key, mask in events:
                handler = key.data
                handler(key.fileobj, mask)

            # This part happens roughly every 20 seconds.
            cur_time = time.time()
            if cur_time - last_report_time > 20:
                self.logger.info('Running report...')
                self.logger.info('Num active peers = {0}'.format(
                    len(self.peername)))
                last_report_time = cur_time


if __name__ == '__main__':
    HOST, PORT = '0.0.0.0', 9049
    try:
        server = AggServer(host=HOST, port=PORT)
        server.serve_forever()
    except KeyboardInterrupt:
        print("Exit...")
