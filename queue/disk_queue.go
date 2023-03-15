package queue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/yhao1206/SMQ/util"
	"log"
	"os"
)

const maxFileSize = 1024 * 1024 * 100

type DiskQueue struct {
	name         string
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	readFile     *os.File
	writeFile    *os.File
	readChan     chan struct{}
	inChan       chan util.ChanReq
	outChan      chan util.ChanRet
	exitChan     chan util.ChanReq
}

func NewDiskQueue(name string) *DiskQueue {
	diskQueue := &DiskQueue{
		name:     name,
		readChan: make(chan struct{}),
		inChan:   make(chan util.ChanReq),
		outChan:  make(chan util.ChanRet),
		exitChan: make(chan util.ChanReq),
	}

	if _, err := os.Stat(diskQueue.metaDataFileName()); err == nil {
		err = diskQueue.retrieveMetaData()
		if err != nil {
			log.Printf("WARNING: failed to retrieveMetaData() - %s", err.Error())
		}
	}

	go diskQueue.router()

	return diskQueue
}

func (d *DiskQueue) Get() ([]byte, error) {
	ret := <-d.outChan
	return ret.Variable.([]byte), ret.Err
}

func (d *DiskQueue) Put(bytes []byte) error {
	errChan := make(chan interface{})
	d.inChan <- util.ChanReq{
		Variable: bytes,
		RetChan:  errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}

func (d *DiskQueue) ReadReadyChan() chan struct{} {
	return d.readChan
}

func (d *DiskQueue) Close() error {
	errChan := make(chan interface{})
	d.exitChan <- util.ChanReq{
		RetChan: errChan,
	}

	err, _ := (<-errChan).(error)
	return err
}

func (d *DiskQueue) readOne() ([]byte, error) {
	var (
		err     error
		msgSize int32
	)
	if d.readPos > maxFileSize {
		d.readFileNum++
		d.readPos = 0
		d.readFile.Close()
		d.readFile = nil

		if err = d.persistMetaData(); err != nil {
			return nil, err
		}
	}

	if d.readFile == nil {
		d.readFile, err = os.OpenFile(d.fileName(d.readFileNum), os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				return nil, err
			}
		}
	}

	err = binary.Read(d.readFile, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, msgSize)
	_, err = d.readFile.Read(readBuf)
	if err != nil {
		return nil, err
	}

	d.readPos += int64(msgSize + 4)

	return readBuf, nil
}

func (d *DiskQueue) writeOne(msg []byte) (err error) {
	var buf bytes.Buffer

	if d.writePos > maxFileSize {
		d.writeFileNum++
		d.writePos = 0
		d.writeFile.Close()
		d.writeFile = nil

		if err = d.persistMetaData(); err != nil {
			return
		}
	}

	if d.writeFile == nil {
		d.writeFile, err = os.OpenFile(d.fileName(d.writeFileNum), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return
		}

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				return
			}
		}
	}

	dataLen := len(msg)
	err = binary.Write(&buf, binary.BigEndian, dataLen)
	if err != nil {
		return
	}

	_, err = buf.Write(msg)
	if err != nil {
		return
	}

	_, err = d.writeFile.Write(buf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return
	}

	d.writePos += int64(dataLen + 4)

	return
}

func (d *DiskQueue) persistMetaData() (err error) {
	metaFileName := d.metaDataFileName()
	tmpFileName := metaFileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	_, err = fmt.Fprintf(f, "%d,%d\n%d,%d\n", d.readFileNum, d.readPos, d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return
	}
	f.Close()

	log.Printf("DISK: persisted meta data for (%s) - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d",
		d.name, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return os.Rename(tmpFileName, metaFileName)
}

func (d *DiskQueue) retrieveMetaData() (err error) {
	metaFileName := d.metaDataFileName()
	f, err := os.OpenFile(metaFileName, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n", &d.readFileNum, &d.readPos, &d.writeFileNum, &d.writePos)
	if err != nil {
		return
	}

	log.Printf("DISK: retrieved meta data for (%s) - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d",
		d.name, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return
}

func (d *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf("%s.diskqueue.meta.dat", d.name)
}

func (d *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf("%s.diskqueue.%06d.dat", d.name, fileNum)
}

func (d *DiskQueue) hasDataToRead() bool {
	return (d.writeFileNum > d.readFileNum) || (d.writePos > d.readPos)
}

func (d *DiskQueue) router() {
	for {
		if d.hasDataToRead() {
			select {
			// in order to read only when we actually want a message
			case d.readChan <- struct{}{}:
				msg, err := d.readOne()
				d.outChan <- util.ChanRet{
					Err:      err,
					Variable: msg,
				}
			case writeRequest := <-d.inChan:
				err := d.writeOne(writeRequest.Variable.([]byte))
				writeRequest.RetChan <- err
			case closeReq := <-d.exitChan:
				if d.readFile != nil {
					d.readFile.Close()
				}
				if d.writeFile != nil {
					d.writeFile.Close()
				}

				closeReq.RetChan <- d.persistMetaData()

				return
			}
		} else {
			select {
			case writeRequest := <-d.inChan:
				err := d.writeOne(writeRequest.Variable.([]byte))
				writeRequest.RetChan <- err
			case closeReq := <-d.exitChan:
				if d.readFile != nil {
					d.readFile.Close()
				}
				if d.writeFile != nil {
					d.writeFile.Close()
				}

				closeReq.RetChan <- d.persistMetaData()

				return
			}
		}
	}
}
