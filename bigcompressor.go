package bigcompressor

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/s2"
)

type BigCompressor struct {
	MaxPrecompressChunkSize int64
	CombineChunk            bool
	buffer                  *bytes.Buffer

	compressFile *os.File
}

type dataChunk struct {
	chunkNumber int
	files       []fileInfo
	totalSize   int64
}

type fileInfo struct {
	os.FileInfo
	file string
}

func (bc *BigCompressor) Compress(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	dataChunks := bc.createChunkInfo(src)

	if bc.CombineChunk {
		var err error
		bc.compressFile, err = os.OpenFile(dst, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0700)
		defer bc.compressFile.Close()
		if err != nil {
			return err
		}
	}
	for _, dataChunk := range dataChunks {
		err := bc.compressChunk(src, &dataChunk)
		if err != nil {
			return err
		}
		if !bc.CombineChunk {
			err = bc.writeChunk(dst + "_" + strconv.Itoa(dataChunk.chunkNumber))
			if err != nil {
				return err
			}
		} else {
			err = bc.writeBufferToFile()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (bc *BigCompressor) Decompress(dst string) error {

	return nil
}

func (bc *BigCompressor) writeBufferToFile() error {
	if _, err := bc.compressFile.Write(bc.buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (bc *BigCompressor) writeChunk(chunkDst string) error {
	fileToWrite, err := os.OpenFile(chunkDst, os.O_CREATE|os.O_RDWR, 0700)
	defer fileToWrite.Close()
	if err != nil {
		return err
	}
	if _, err := io.Copy(fileToWrite, bc.buffer); err != nil {
		return err
	}
	return nil
}

func (bc *BigCompressor) compressChunk(src string, chunk *dataChunk) error {
	if bc.buffer == nil {
		bc.buffer = &bytes.Buffer{}
	}
	bc.buffer.Reset()
	zr := s2.NewWriter(bc.buffer)
	tw := tar.NewWriter(zr)
	for _, fi := range chunk.files {
		header, err := tar.FileInfoHeader(fi, fi.file)

		if err != nil {
			return err
		}
		header.Name = strings.Replace(fi.file, src, "", 1)

		// write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(fi.file)
			defer data.Close()
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
	}

	// produce tar
	if err := tw.Close(); err != nil {
		return err
	}
	// produce gzip
	if err := zr.Close(); err != nil {
		return err
	}
	return nil
}

func (bc BigCompressor) createChunkInfo(src string) []dataChunk {
	dataChunks := []dataChunk{}
	currentChunk := 0

	filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		if len(dataChunks)-1 != currentChunk {
			dataChunks = append(dataChunks, dataChunk{
				chunkNumber: currentChunk,
				files:       []fileInfo{},
				totalSize:   0,
			})
		}
		chunk := &dataChunks[currentChunk]
		if chunk.totalSize+fi.Size() <= bc.MaxPrecompressChunkSize {
			chunk.files = append(chunk.files, fileInfo{
				file:     file,
				FileInfo: fi,
			})
			if !fi.IsDir() {
				chunk.totalSize += fi.Size()
			}
		} else {
			currentChunk++
		}
		return nil
	})
	return dataChunks
}
