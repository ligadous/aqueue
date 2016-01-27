# AQUEUE - Append Only Queue

Queue append only and readable

ONE FILE - this version dont rotate files

Dir dbdir

POP     Dir dbdir/pop
PUSH    Dir dbdir/push

All files are append only


//Msg append only file system
//Length +  CRC(payload)    + Payload
//int32 +   byte        +   int32 + n bytes
//(1+4+n)
