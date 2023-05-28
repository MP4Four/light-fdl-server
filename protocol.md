# Protocol Spec
## 1. UPLOAD : client --> server : goto 2
(ssize: 8(ulong), client_id: 8(str), epoch: 4(int), precision: 4(float), reserved: 8(bytes), obj: ssize(bytes))
32 bytes + ssize bytes

Aggregate......

## 2. AGG finish : server --> client : goto 3
(status: 2(bytes)

[AGG_FINISH: \x00\x00]

## 3. FETCH : server --> client
(ssize: 8(ulong), obj: ssize(bytes))