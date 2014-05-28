package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
)

type Cdb struct {
	fp     *os.File
	bucket [][]HashPointer
	p      uint32 //global pointer, used to track file position
	loop   uint32 // number of hash slots searched under this key
	khash  uint32 // initialized if loop is nonzero
	kpos   uint32 // initialized if loop is nonzero
	hpos   uint32 // initialized if loop is nonzero
	hslots uint32 // initialized if loop is nonzero
	dpos   uint32 // initialized if FindNext() returns true
	dlen   uint32 // initialized if FindNext() returns true
}


func New(fileName string) (*Cdb, error) {
	
	c := new(Cdb)

	fp, err := os.Create(fileName)
	if err != nil {
		fmt.Errorf("hata:%s", err)
		return c, err
	}

	c.fp = fp

	pos_header, err := c.fp.Seek(0, os.SEEK_CUR)
	if err != nil {
		fmt.Errorf("hata:%s\n", err)
		return c, err
	}
	fmt.Printf("pos_header:%d", pos_header)

	// skip header
	c.p = uint32(pos_header + (4+4)*256) // sizeof((h,p))*256
	//fmt.Printf("p:%d\n", p)
	c.fp.Seek(int64(c.p), 0)

	c.bucket = make([][]HashPointer, 256, 256)

	return c, nil
}


func(c *Cdb) Add(key, value []byte) {
	c.fp.Write(uint32ToBytes(uint32(len(key))))
	c.fp.Write(uint32ToBytes(uint32(len(value))))
	c.fp.Write(key)
	c.fp.Write(value)
	h := hash(key)
	c.bucket[h%256] = append(c.bucket[h%256], HashPointer{h, uint32(c.p)})
	c.p += 4 + 4 + uint32(len(key)) + uint32(len(value))
}

func(c *Cdb) Close() {
	pos_hash := c.p

	// write hashes
	for _, b := range c.bucket {
		//fmt.Println(b)
		if len(b) > 0 {
			ncells := uint32(len(b) * 2)
			cells := make([]HashPointer, ncells, ncells)
			for _, hp := range b {
				i := (hp.hash >> 8) % ncells

				//is call already occupied?
				for cells[i].pointer > 0 {
					i = (i + 1) % ncells
				}
				cells[i] = HashPointer{hp.hash, hp.pointer}
			}

			for _, cell := range cells {
				c.fp.Write(uint32ToBytes(cell.hash))
				c.fp.Write(uint32ToBytes(cell.pointer))
			}
		}
	}

	var pos_header int64 = 0
	//fmt.Println(bucket)
	// write header
	c.fp.Seek(pos_header, 0)
	//fmt.Printf("now file cursor at:%d\n", pos_header)
	for _, b := range c.bucket {
		c.fp.Write(uint32ToBytes(pos_hash))
		c.fp.Write(uint32ToBytes(uint32(len(b) * 2)))
		pos_hash += uint32((len(b) * 2) * (4 + 4))
	}

	c.fp.Close()
}

type HashPointer struct {
	hash, pointer uint32
}

var a = map[string]string{}

func hash(b []byte) uint32 {
	var h uint32 = 5381

	for i := 0; i < len(b); i++ {
		h = ((h << 5) + h) ^ uint32(b[i])
	}
	return h
}

func uint32ToBytes(x uint32) []byte {
	var buf [4]byte
	buf[0] = byte(x >> 0)
	buf[1] = byte(x >> 8)
	buf[2] = byte(x >> 16)
	buf[3] = byte(x >> 24)
	return buf[:]
}

func bytesToUint32le(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func write_cdb(fileName string) {
	fp, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Errorf("hata:%s", err)
	}

	pos_header, err := fp.Seek(0, os.SEEK_CUR)
	if err != nil {
		fmt.Errorf("hata:%s\n", err)
	}
	fmt.Printf("pos_header:%d", pos_header)

	// skip header
	p := uint32(pos_header + (4+4)*256) // sizeof((h,p))*256
	//fmt.Printf("p:%d\n", p)
	fp.Seek(int64(p), 0)

	bucket := make([][]HashPointer, 256, 256)

	for k, v := range a {
		//fmt.Println(k, v)
		fp.Write(uint32ToBytes(uint32(len(k))))
		fp.Write(uint32ToBytes(uint32(len(v))))
		fp.Write([]byte(k))
		fp.Write([]byte(v))
		h := hash([]byte(k))
		bucket[h%256] = append(bucket[h%256], HashPointer{h, uint32(p)})
		p += 4 + 4 + uint32(len(k)) + uint32(len(v))
	}

	//fmt.Println(bucket)

	pos_hash := p

	// write hashes
	for _, b := range bucket {
		//fmt.Println(b)
		if len(b) > 0 {
			ncells := uint32(len(b) * 2)
			cells := make([]HashPointer, ncells, ncells)
			for _, hp := range b {
				i := (hp.hash >> 8) % ncells

				//is call already occupied?
				for cells[i].pointer > 0 {
					i = (i + 1) % ncells
				}
				cells[i] = HashPointer{hp.hash, hp.pointer}
			}

			for _, cell := range cells {
				fp.Write(uint32ToBytes(cell.hash))
				fp.Write(uint32ToBytes(cell.pointer))
			}
		}
	}

	//fmt.Println(bucket)
	// write header
	fp.Seek(pos_header, 0)
	//fmt.Printf("now file cursor at:%d\n", pos_header)
	for _, b := range bucket {
		fp.Write(uint32ToBytes(pos_hash))
		fp.Write(uint32ToBytes(uint32(len(b) * 2)))
		pos_hash += uint32((len(b) * 2) * (4 + 4))
	}

	fp.Close()
}

func Find(key []byte) (result [][]byte, err error) {

	b := make([]byte, 4, 4)
	//fmt.Println(b)

	fp, err := os.Open("test.db")
	if err != nil {
		fmt.Errorf("hata:%s", err)
	}

	r := make([][]byte, 0, 1)
	//fmt.Println(r)
	h := hash(key)

	fp.Seek(int64((h%256)*(4+4)), 0)

	fp.Read(b)
	//fmt.Println(b)
	pos_bucket := bytesToUint32le(b)

	fp.Read(b)
	//fmt.Println(b)
	ncells := bytesToUint32le(b)
	//fmt.Println(ncells)

	//if ncells == 0: raise KeyError
	if ncells == 0 {
		return r, nil
	}

	// return r
	start := (h >> 8) % ncells
	//fmt.Println("start:", start)

	var i uint32

	for i = 0; i < ncells; i++ {
		fp.Seek(int64(pos_bucket+((start+i)%ncells)*(4+4)), 0)

		fp.Read(b)
		//fmt.Println("h1 bytes:", b)
		h1 := bytesToUint32le(b)
		//fmt.Println("h1:", h1)

		fp.Read(b)
		//fmt.Println("p1 bytes:", b)
		p1 := bytesToUint32le(b)
		//fmt.Println("p1:", p1)

		if p1 == 0 {
			return r, nil
		}

		//fmt.Println("h:", h)
		if h1 == h {
			fp.Seek(int64(p1), 0)

			fp.Read(b)
			klen := bytesToUint32le(b)
			fp.Read(b)
			vlen := bytesToUint32le(b)

			key_buffer := make([]byte, klen, klen)
			fp.Read(key_buffer)
			//fmt.Println("key_buffer:", key_buffer)

			val_buffer := make([]byte, vlen, vlen)
			fp.Read(val_buffer)
			//fmt.Println("val_buffer:", val_buffer)

			if bytes.Equal(key_buffer, key) {
				r = append(r, val_buffer)
				//fmt.Println(r)
			}
		}
	}
	return r, nil
}

func main() {
	
	cdb, err := New("test.db")
	if err!=nil {
		fmt.Println(err)
	}

	for i := 0; i < 1000000; i++ {
		cdb.Add([]byte("key"+strconv.Itoa(i)), []byte("value" + strconv.Itoa(i)))
		//a["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i)
	}

	cdb.Close()

	//write_cdb("fileName.db")

	r, _ := Find([]byte("key34444"))
	fmt.Println(string(r[0]))

}
