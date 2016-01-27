package aqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	TIMETOFLUSH = 1e9
	MAXSIZE     = 100000000
	DQDIR       = "db"

	DIRPOP  = "pop"
	DIRPUSH = "push"
	DIROLD  = "old"

	HEADERSZ = 22
	DELRECSZ = 43
)

// Data Struct
// Contém os registros do Push
type Data struct {
	Offset  uint32
	Payload []byte
}

//Msg append only file system
//Length +  CRC(payload)    + Payload
//int32 +   byte        +   int32 + n bytes
//(1+4+n)

// Timestamp into payload
// In-memory control active segments
type Record struct {
	Len     int32
	Crc     uint32
	Payload []byte
}

// uint32 + int64 (4+8) = 12 bytes
// offset crc + offset last pop record
type RecordPop struct {
	Crc     uint32 //10 + space
	Offset  int64  //20 + space
	CrcPush uint32 //10 + \n
}

type DataChannel struct {
	Command string
	Param   []byte
}

type DataChannelResponse struct {
	Response []byte
	Err      error
}

type DQueue struct {
	sdir string

	pushNro    uint64 //Push file name
	pushRecNro uint64 //Current number of records
	pushFH     *os.File

	pushReadNro    uint64 //Push file name for read
	pushReadFH     *os.File
	pushReadOffset int64

	popNro    uint64 //Pop file name
	popRecNro uint64 //Current number of records
	popFH     *os.File

	Mu *sync.RWMutex

	maxsize uint64

	dataChannel         chan DataChannel
	dataChannelResponse chan DataChannelResponse

	clearStarted bool
}

func New(dbdir string) (dq *DQueue, err error) {
	sdir := dbdir

	var m sync.RWMutex

	ch := make(chan DataChannel)
	chResponse := make(chan DataChannelResponse)
	dq = &DQueue{sdir, 0, 0, nil, 0, nil, 0, 0, 0, nil, &m, MAXSIZE, ch, chResponse, false}

	//Dir structures
	err = os.MkdirAll(sdir, 0766)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro MkdirAll %s", err))
	}

	//Dir structures
	err = os.MkdirAll(filepath.Join(sdir, ".bkp"), 0766)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro MkdirAll %s", err))
	}

	err = dq.Open()
	if err != nil {
		return nil, err
	}

	go dq.Deliver()
	dq.pushReadOffset, err = dq.ReadDelRecord()

	//pushList = dq.PopReadOnlyStart()

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro Last record %s", err))
	}

	go dq.flush()

	return dq, nil
}

func (dq *DQueue) Deliver() {

	for s := range dq.dataChannel {

		switch s.Command {
		case "push":
			err := dq.push(s.Param)
			dcr := DataChannelResponse{nil, err}
			dq.dataChannelResponse <- dcr

		case "pop":
			res, err := dq.pop()
			dcr := DataChannelResponse{res, err}
			dq.dataChannelResponse <- dcr

		default:
		}
	}
}

func (dq *DQueue) Pop() ([]byte, error) {
	d := DataChannel{"pop", nil}
	dq.dataChannel <- d
	dcr := <-dq.dataChannelResponse
	return dcr.Response, dcr.Err
}

func (dq *DQueue) Push(s []byte) error {
	d := DataChannel{"push", s}
	dq.dataChannel <- d
	dcr := <-dq.dataChannelResponse
	return dcr.Err
}

func (dq *DQueue) MaxSize(m uint64) {
	dq.maxsize = m
}

//Disk Storage Close
func (dq *DQueue) Open() error {
	var err error
	sdirpop := dq.sdir + "/pop"
	sdirpush := dq.sdir + "/push"

	dq.popFH, err = os.OpenFile(sdirpop, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open pop %s", err))
	}

	dq.pushFH, err = os.OpenFile(sdirpush, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open push %s", err))
	}

	dq.pushReadFH, err = os.OpenFile(sdirpush, os.O_RDONLY, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open push read  read %s", err))
	}
	return nil
}

//Disk Storage Close
func (dq *DQueue) Close() error {
	var err error
	if dq.pushFH != nil {
		err = dq.pushFH.Close()
	}
	if dq.pushReadFH != nil {
		err = dq.pushReadFH.Close()
	}
	if dq.popFH != nil {
		err = dq.popFH.Close()
	}

	return err
}

//Flush Disk
func (dq *DQueue) flush() {
	for {
		time.Sleep(TIMETOFLUSH)
		dq.Mu.Lock()
		dq.popFH.Sync()
		dq.pushFH.Sync()
		dq.Mu.Unlock()
	}
}

//Rotate
//Verify number of record of lastfilename
//TODO: Better error handler
func (dq *DQueue) findpushRecNro() (nro uint64, err error) {
	//Using push last file
	curoffset, err := dq.pushFH.Seek(0, 0) //Begin
	if err == io.EOF {
		err = nil
		return
	}

	//Loop of Records
	for {
		//Read Header
		rec, err := dq.ReadHeader()
		if err == io.EOF {
			err = nil
			break
		}
		//Read Payload
		err = dq.ReadPayload(rec)
		if err == io.EOF {
			err = nil
			break
		}
		data := rec.Payload
		//Update offset
		curoffset += int64(HEADERSZ + len(data))
		nro++
	}

	return
}

func (dq *DQueue) findpopRecNro() (nro uint64, err error) {
	//Using push last file
	_, err = dq.popFH.Seek(0, 0) //Begin
	if err == io.EOF {
		err = nil
		return
	}
	//Loop of Records
	for {
		//Read Del Records
		_, err := dq.offsetDelRecord()
		if err == io.EOF {
			err = nil
			break
		}
		nro++
	}
	return
}

func sliceCRC(data []byte) uint32 {
	buff := new(bytes.Buffer)
	buff.Write(data)
	return crc32.ChecksumIEEE(buff.Bytes())
}

func int64CRC(data int64) uint32 {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, data)
	return crc32.ChecksumIEEE(buff.Bytes())
}

//PUSH =================================================================
//Write a data into record
func (dq *DQueue) push(data []byte) (err error) {
	dq.Mu.Lock()
	defer dq.Mu.Unlock()

	//Acrescentar  no final |+ \n
	data = append(data, []byte("\n")...)

	rec, err := buildPushRecord(data)
	if err != nil {
		return
	}
	err = dq.pushRecord(rec)
	if err != nil {
		return
	}
	dq.pushRecNro++

	return
}

func buildPushRecord(data []byte) (r *Record, err error) {
	//payload length
	sz := int32(len(data))
	if sz == 0 {
		err = errors.New("Data is empty")
		return
	}
	//Calculating CRC32
	crc := sliceCRC(data)
	r = &Record{sz, crc, data}
	return
}

func (dq *DQueue) pushRecord(r *Record) error {
	buff := new(bytes.Buffer)
	//More readable (20 bytes)
	buff.Write([]byte(fmt.Sprintf("%010d", r.Len)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%010d", r.Crc)))
	buff.Write([]byte(" "))
	buff.Write(r.Payload)
	_, err := dq.pushFH.Write(buff.Bytes())
	return err
}

type RecData struct {
	Data []byte
}

//POP READ ONLY ========================================================
func (dq *DQueue) PopReadOnly(nroRec uint64) (data [][]byte, nroRecDone uint64) {
	dq.Mu.Lock()
	defer func(d *[][]byte) {
		if nroRec != 0 && uint64(dq.pushReadOffset) > dq.maxsize && len(*d) == 0 {
			dq.rotatePop()
		}
		dq.Mu.Unlock()
	}(&data)

	dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)

	for {
		//Read Header
		rec, err := dq.ReadHeader()
		if err != nil {
			return
		}
		//Read Payload
		err = dq.ReadPayload(rec)
		if err != nil {
			return
		}

		paylo := rec.Payload
		le := len(paylo)

		data = append(data, paylo[:le-1])

		nroRecDone++
		if nroRecDone >= nroRec {
			break
		}
	}
	return
}

func (dq *DQueue) PopReadOnlyTail(nroRec uint64) (data [][]byte, nroRecDone uint64) {
	dq.Mu.Lock()
	defer dq.Mu.Unlock()

	dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)

	for {
		//Read Header
		rec, err := dq.ReadHeader()
		if err != nil {
			return
		}
		//Read Payload
		err = dq.ReadPayload(rec)
		if err != nil {
			return
		}

		paylo := rec.Payload
		le := len(paylo)
		if uint64(len(data)) >= nroRec {
			data = data[1:]
		}
		data = append(data, paylo[:le-1])
		nroRecDone = uint64(len(data))
	}
	return
}

func (dq *DQueue) PopReadOnlyStart() (data []Data) {
	dq.Mu.Lock()
	defer dq.Mu.Unlock()

	//dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)
	offsetCurr := dq.pushReadOffset
	for {
		of, err := dq.pushReadFH.Seek(offsetCurr, os.SEEK_SET)
		if err != nil {
			fmt.Printf("%v", err.Error())
		}
		fmt.Printf("%d\n", of)
		//Read Header
		rec, err := dq.ReadHeader()
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			return
		}
		//Read Payload
		err = dq.ReadPayload(rec)
		if err != nil {
			return
		}

		paylo := rec.Payload

		le := len(paylo)
		offset := int64(offsetCurr) + int64(HEADERSZ) + int64(le)
		d := Data{uint32(offset), []byte(paylo)}

		data = append(data, d)
		offsetCurr += offset
	}
	return
}

//POP =================================================================
func (dq *DQueue) pop() (data []byte, err error) {
	dq.Mu.Lock()
	defer dq.Mu.Unlock()
	dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)
	//Read Header
	rec, err := dq.ReadHeader()

	if err == io.EOF && uint64(dq.pushReadOffset) > dq.maxsize {
		dq.rotatePop()
	}

	if err != nil {
		return
	}
	//Read Payload
	err = dq.ReadPayload(rec)
	if err != nil {
		return
	}

	paylo := rec.Payload
	le := len(paylo)
	data = append(data, paylo[:le-1]...)

	//Update offset
	dq.pushReadOffset += int64(HEADERSZ + len(paylo))
	//Write POP record
	err = dq.WriteDelRecord(dq.pushReadOffset, rec.Crc)
	if err != nil {
		return
	}
	dq.popRecNro++

	return
}

func compressFile(fn string) error {
	_, err := exec.Command("gzip", fn).Output()
	if err != nil {
		return err
	}

	return nil
}

func (dq *DQueue) rotatePop() error {
	//Rotate
	//Close atual, move and open
	dq.Close()

	//Move
	sdirpopOrigin := filepath.Join(dq.sdir, "pop")
	sdirpushOrigin := filepath.Join(dq.sdir, "push")
	sdirpopDestine := filepath.Join(dq.sdir, ".bkp", "pop"+fmt.Sprintf("%s", time.Now().Format("20060102_150405")))
	sdirpushDestine := filepath.Join(dq.sdir, ".bkp", "push"+fmt.Sprintf("%s", time.Now().Format("20060102_150405")))
	os.Rename(sdirpopOrigin, sdirpopDestine)
	os.Rename(sdirpushOrigin, sdirpushDestine)

	compressFile(sdirpopDestine)
	compressFile(sdirpushDestine)

	dq.Open()

	//Zera Ponteiros
	dq.pushReadOffset = 0
	dq.pushNro = 0
	dq.pushRecNro = 0
	dq.pushReadNro = 0
	dq.popNro = 0
	dq.popRecNro = 0
	return nil
}

func (dq *DQueue) WriteDelRecord(off int64, crcpush uint32) error {
	crc := int64CRC(off)
	buff := new(bytes.Buffer)
	buff.Write([]byte(fmt.Sprintf("%010d", crc)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%020d", off)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%010d", crcpush)))
	buff.Write([]byte("\n"))
	_, err := dq.popFH.Write(buff.Bytes())
	return err
}

//Read last del record
func (dq *DQueue) ReadDelRecord() (offset int64, err error) {
	//File is Empty
	curoffset, err := dq.popFH.Seek(0, 2)
	if curoffset == 0 {
		return
	}
	curoffset, err = dq.popFH.Seek(-DELRECSZ, 2)
	if err == io.EOF {
		err = nil
		return
	}
	offset, err = dq.offsetDelRecord()
	return
}

// Find offset of delRecord in current position
func (dq *DQueue) offsetDelRecord() (offset int64, err error) {
	var databuff []byte = make([]byte, DELRECSZ)
	datasz, err := dq.popFH.Read(databuff)
	if err != nil {
		return
	}
	if int32(datasz) != DELRECSZ {
		err = errors.New(fmt.Sprintf("Invalid DEL RECORD size. Expected %d got %d bytes", DELRECSZ, int32(datasz)))
		return
	}
	var crc uint32

	scrc := fmt.Sprintf("%s", databuff[0:10])
	//space 1 byte
	soff := fmt.Sprintf("%s", databuff[11:31])
	//newline 1 byte
	offset, _ = strconv.ParseInt(soff, 10, 64)
	nCrc, _ := strconv.ParseUint(scrc, 10, 0)
	crc = uint32(nCrc)

	checkcrc := int64CRC(offset)
	if checkcrc != crc {
		err = errors.New(fmt.Sprintf("Invalid last record CRC"))
		return
	}

	return
}

func (dq *DQueue) ReadHeader() (rec *Record, err error) {
	rec, err = dq.readHeader(dq.pushReadFH)
	return
}

func (dq *DQueue) readHeader(fh *os.File) (rec *Record, err error) {
	rec = &Record{0, 0, nil}
	var headbuff []byte = make([]byte, HEADERSZ)
	headsz, err := fh.Read(headbuff)
	if err != nil {
		return
	}
	if int32(headsz) != HEADERSZ {
		err = errors.New(fmt.Sprintf("Invalid Header size. Expected %d got %d bytes", HEADERSZ, int32(headsz)))
		return
	}

	slen := fmt.Sprintf("%s", headbuff[0:10])
	scrc := fmt.Sprintf("%s", headbuff[11:21])

	nLen, _ := strconv.Atoi(slen)
	nCrc, _ := strconv.ParseUint(scrc, 10, 0)

	rec.Len = int32(nLen)
	rec.Crc = uint32(nCrc)

	return
}

func (dq *DQueue) ReadPayload(rec *Record) (err error) {
	err = dq.readPayload(dq.pushReadFH, rec)
	return
}

func (dq *DQueue) readPayload(fh *os.File, rec *Record) (err error) {
	var databuff []byte = make([]byte, rec.Len)
	var sz int
	sz, err = fh.Read(databuff)
	if err != nil {
		return
	}
	if int32(sz) != rec.Len {
		err = errors.New(fmt.Sprintf("Invalid Data size. Expected %d got %d bytes", rec.Len, sz))
		return
	}
	rec.Payload = databuff
	payloadCrc := sliceCRC(databuff)
	if rec.Crc != payloadCrc {
		err = errors.New("CRC not equal!")
	}
	return
}

func (dq *DQueue) SetCleanerTime(d time.Duration) {
	if !dq.clearStarted {
		dq.clearStarted = true
		go dq.cleanerProcess(d)
	}
}

func (dq *DQueue) cleanerProcess(d time.Duration) {
	pathbkp := filepath.Join(dq.sdir, ".bkp")
	for {
		dirs, err := ioutil.ReadDir(pathbkp)
		if err != nil {
			// not found dirs
			time.Sleep(time.Hour)
			continue
		}
		for _, dir := range dirs {
			if !dir.IsDir() {
				if time.Since(dir.ModTime()) > d {
					os.Remove(dir.Name())
				}
			}
		}
		time.Sleep(time.Hour * 24)
	}
}

//UTILS (copyfile)
func CopyFile(dst, src string) (int64, error) {
	sf, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer sf.Close()
	df, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer df.Close()
	return io.Copy(df, sf)
}

//DIRECTORY =================================================================
// A fileInfoList implements sort.Interface.
type fileInfoList []os.FileInfo

func (f fileInfoList) Len() int { return len(f) }
func (f fileInfoList) Less(i, j int) bool {
	a, _ := strconv.ParseUint(f[i].Name(), 10, 64)
	b, _ := strconv.ParseUint(f[j].Name(), 10, 64)
	return a < b
}
func (f fileInfoList) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// ReadDir reads the directory named by dirname and returns
// a list of sorted directory entries.
// NUMERIC Order
func readDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	fi := make(fileInfoList, len(list))
	for i := range list {
		//fi[i] = &list[i]
		fi[i] = list[i]
	}
	sort.Sort(fi)
	return fi, nil
}

func lastFile(dir string) (fileNro uint64, err error) {
	var lastfile string

	//Verify directory
	listdir, err := readDir(dir)
	if err != nil {
		return
	}
	numFile := len(listdir)
	if numFile == 0 {
		lastfile = "1"
	} else {
		lenlist := numFile - 1
		lastfile = listdir[lenlist].Name()
	}

	fileNro, err = strconv.ParseUint(lastfile, 10, 64)
	return
}
