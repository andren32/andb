package core

import (
	"encoding/binary"
	"hash/crc32"
)

func CRCForSlice(slice []byte) []byte {
	crc := crc32.ChecksumIEEE(slice)
	crcAsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcAsBytes, crc)
	return crcAsBytes
}
